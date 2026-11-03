/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jms.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jms.client.JmsConnectionFactories;
import org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.DeliveryMode;
import org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.MessageType;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorException;

import lombok.extern.slf4j.Slf4j;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.DELIVERY_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.DESTINATION;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.MAX_UNCOMMITTED_MESSAGES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.MESSAGE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.PRIORITY;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.TIME_TO_LIVE;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.CLOSE_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.COMMIT_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.CONNECTION_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode.SEND_FAILED;

/**
 * Sends rows to one queue in a transacted session.
 *
 * <p>Messages become visible when the session is committed: before every checkpoint ({@link
 * #prepareCommit(long)}) and whenever {@code max_uncommitted_messages} have been sent. Messages of
 * an uncommitted transaction are discarded when the connection closes or the engine aborts the
 * commit, and the engine replays the rows after the last checkpoint, so delivery is at least once.
 *
 * <p>All methods except {@link #close()} are called by the task thread, so the session is never
 * used concurrently.
 */
@Slf4j
public class JmsSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final ReadonlyConfig config;
    private final SerializationSchema serializer;
    private final String destination;
    private final MessageType messageType;
    private final int maxUncommittedMessages;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private ConnectionFactory connectionFactory;
    private volatile Connection connection;
    private Session session;
    private MessageProducer producer;
    private int uncommitted;

    public JmsSinkWriter(ReadonlyConfig config, SerializationSchema serializer) {
        this(config, serializer, null);
    }

    JmsSinkWriter(
            ReadonlyConfig config,
            SerializationSchema serializer,
            ConnectionFactory connectionFactory) {
        this.config = config;
        this.serializer = serializer;
        this.connectionFactory = connectionFactory;
        this.destination = config.get(DESTINATION);
        this.messageType = config.get(MESSAGE_TYPE);
        this.maxUncommittedMessages = config.get(MAX_UNCOMMITTED_MESSAGES);
    }

    /**
     * Sends the row as one message in the open transaction. The first row opens the connection, and
     * the transaction is committed once it holds {@code max_uncommitted_messages} messages.
     */
    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkFailure();
        open();
        byte[] body;
        try {
            body = serializer.serialize(element);
        } catch (RuntimeException e) {
            // Serialization errors may quote the row, so neither their message nor the cause is
            // kept.
            throw new JmsConnectorException(
                    SEND_FAILED,
                    "Could not serialize a row for JMS destination "
                            + destination
                            + " ("
                            + e.getClass().getName()
                            + "); check the format option");
        }
        try {
            producer.send(message(body));
        } catch (JMSException e) {
            throw fail(
                    new JmsConnectorException(
                            SEND_FAILED, "Could not send to JMS destination " + destination, e));
        }
        uncommitted++;
        if (uncommitted >= maxUncommittedMessages) {
            commit("max_uncommitted_messages was reached");
        }
    }

    private Message message(byte[] body) throws JMSException {
        if (messageType == MessageType.BYTES) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(body);
            return message;
        }
        return session.createTextMessage(new String(body, StandardCharsets.UTF_8));
    }

    /**
     * Commits the messages sent since the last commit, so they are on the broker before the
     * checkpoint.
     */
    @Override
    public Optional<Void> prepareCommit(long checkpointId) throws IOException {
        commitPending("checkpoint " + checkpointId);
        return Optional.empty();
    }

    /** Same as {@link #prepareCommit(long)}, for a commit request without a checkpoint id. */
    @Override
    public Optional<Void> prepareCommit() throws IOException {
        commitPending("a commit request without a checkpoint id");
        return Optional.empty();
    }

    private void commitPending(String reason) {
        if (uncommitted > 0) {
            checkFailure();
            commit(reason);
        }
    }

    private void commit(String reason) {
        try {
            session.commit();
        } catch (JMSException e) {
            throw fail(
                    new JmsConnectorException(
                            COMMIT_FAILED,
                            "Could not commit the messages sent to JMS destination "
                                    + destination
                                    + " for "
                                    + reason,
                            e));
        }
        uncommitted = 0;
    }

    /** Discards the uncommitted messages. Only Spark calls it, when a task commit fails. */
    @Override
    public void abortPrepare() {
        if (session == null) {
            return;
        }
        try {
            session.rollback();
        } catch (JMSException e) {
            // The connection may be lost; its uncommitted transaction is discarded with it.
            log.warn("Could not roll back the JMS transaction of destination {}", destination, e);
        }
        uncommitted = 0;
    }

    /** Opens the connection, the transacted session and the producer once. */
    private void open() {
        if (producer != null) {
            return;
        }
        try {
            if (connectionFactory == null) {
                connectionFactory = JmsConnectionFactories.create(config);
            }
            Connection created = JmsConnectionFactories.createConnection(connectionFactory, config);
            connection = created;
            created.setExceptionListener(e -> failure.compareAndSet(null, e));
            session = created.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer newProducer = session.createProducer(session.createQueue(destination));
            newProducer.setDeliveryMode(
                    config.get(DELIVERY_MODE) == DeliveryMode.PERSISTENT
                            ? javax.jms.DeliveryMode.PERSISTENT
                            : javax.jms.DeliveryMode.NON_PERSISTENT);
            newProducer.setPriority(config.get(PRIORITY));
            newProducer.setTimeToLive(config.get(TIME_TO_LIVE));
            producer = newProducer;
        } catch (JMSException e) {
            throw fail(
                    new JmsConnectorException(
                            CONNECTION_FAILED,
                            "Could not open a producer for JMS destination " + destination,
                            e));
        } catch (RuntimeException e) {
            throw fail(e);
        }
    }

    private void checkFailure() {
        Throwable error = failure.get();
        if (error != null) {
            throw new JmsConnectorException(
                    CONNECTION_FAILED,
                    "The JMS sink writer failed; uncommitted messages are discarded",
                    error);
        }
    }

    private RuntimeException fail(RuntimeException error) {
        failure.compareAndSet(null, error);
        return error;
    }

    /** Closes the connection, which discards an uncommitted transaction. */
    @Override
    public void close() throws IOException {
        Connection current = connection;
        connection = null;
        if (current == null) {
            return;
        }
        try {
            current.close();
        } catch (JMSException e) {
            if (failure.get() == null) {
                throw new JmsConnectorException(
                        CLOSE_FAILED, "Could not close the JMS connection", e);
            }
            log.warn("Could not close the JMS connection after a failure", e);
        }
    }
}
