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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jms.JmsTestBrokers;
import org.apache.seatunnel.connectors.seatunnel.jms.JmsTestBrokers.Provider;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jms.exception.JmsConnectorException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the sink writer against real brokers embedded in the test JVM. */
class JmsSinkWriterTest {

    private static final long EMPTY_QUEUE_WAIT_MILLIS = 3000;
    private static final long MESSAGE_WAIT_MILLIS = 15000;

    private final List<JmsSinkWriter> writers = new ArrayList<>();
    private String queue;

    @BeforeAll
    static void startBrokers() throws Exception {
        JmsTestBrokers.start();
    }

    @AfterAll
    static void stopBrokers() throws Exception {
        JmsTestBrokers.stop();
    }

    @BeforeEach
    void newQueue() {
        queue = "jms-sink-" + UUID.randomUUID();
    }

    @AfterEach
    void closeWriters() {
        for (JmsSinkWriter writer : writers) {
            try {
                writer.close();
            } catch (Exception ignored) {
                // A failed writer may fail to close; the test has already checked what it needs.
            }
        }
    }

    static List<Provider> providers() {
        return JmsTestBrokers.providers();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void messagesBecomeVisibleWhenPrepareCommitCommitsThem(Provider provider) throws Exception {
        JmsSinkWriter writer = writer(provider, Collections.emptyMap(), null);
        writer.write(row(1));
        writer.write(row(2));

        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));

        writer.prepareCommit(1);

        assertEquals(Arrays.asList(json(1), json(2)), provider.drain(queue, MESSAGE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void closeDiscardsUncommittedMessages(Provider provider) throws Exception {
        JmsSinkWriter writer = writer(provider, Collections.emptyMap(), null);
        writer.write(row(1));
        writer.prepareCommit(1);
        writer.write(row(2));
        writer.close();

        assertEquals(
                Collections.singletonList(json(1)), provider.drain(queue, MESSAGE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void abortPrepareDiscardsUncommittedMessages(Provider provider) throws Exception {
        JmsSinkWriter writer = writer(provider, Collections.emptyMap(), null);
        writer.write(row(1));
        writer.abortPrepare();
        writer.prepareCommit(1);
        writer.write(row(2));
        writer.prepareCommit(2);

        assertEquals(
                Collections.singletonList(json(2)), provider.drain(queue, MESSAGE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void maxUncommittedMessagesCommitsBeforeTheCheckpoint(Provider provider) throws Exception {
        JmsSinkWriter writer =
                writer(provider, Collections.singletonMap("max_uncommitted_messages", 2), null);
        for (int id = 1; id <= 5; id++) {
            writer.write(row(id));
        }

        assertEquals(
                Arrays.asList(json(1), json(2), json(3), json(4)),
                provider.drain(queue, MESSAGE_WAIT_MILLIS));

        writer.prepareCommit(1);

        assertEquals(
                Collections.singletonList(json(5)), provider.drain(queue, MESSAGE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void messageTypeAndProducerSettingsAreApplied(Provider provider) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("message_type", "BYTES");
        options.put("delivery_mode", "NON_PERSISTENT");
        options.put("priority", 7);
        options.put("time_to_live", 600000L);
        JmsSinkWriter writer = writer(provider, options, null);
        long sent = System.currentTimeMillis();
        writer.write(row(1));
        writer.prepareCommit(1);

        Message message = receiveOne(provider);
        assertTrue(message instanceof BytesMessage, message.getClass().getName());
        assertEquals(json(1), JmsTestBrokers.body(message));
        assertEquals(DeliveryMode.NON_PERSISTENT, message.getJMSDeliveryMode());
        assertEquals(7, message.getJMSPriority());
        assertTrue(message.getJMSExpiration() >= sent + 600000L, "expiration not set");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void defaultsSendPersistentTextMessagesWithoutExpiration(Provider provider) throws Exception {
        JmsSinkWriter writer = writer(provider, Collections.emptyMap(), null);
        writer.write(row(1));
        writer.prepareCommit(1);

        Message message = receiveOne(provider);
        assertTrue(message instanceof TextMessage, message.getClass().getName());
        assertEquals(DeliveryMode.PERSISTENT, message.getJMSDeliveryMode());
        assertEquals(4, message.getJMSPriority());
        assertEquals(0, message.getJMSExpiration());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void failedCommitFailsTheWriterAndDiscardsTheMessages(Provider provider) throws Exception {
        AtomicBoolean failCommits = new AtomicBoolean(true);
        JmsSinkWriter writer =
                writer(
                        provider,
                        Collections.emptyMap(),
                        commitFailingFactory(provider.factory(), failCommits));
        writer.write(row(1));

        JmsConnectorException error =
                assertThrows(JmsConnectorException.class, () -> writer.prepareCommit(1));
        assertEquals(JmsConnectorErrorCode.COMMIT_FAILED, error.getSeaTunnelErrorCode());
        assertThrows(JmsConnectorException.class, () -> writer.write(row(2)));
        writer.close();

        assertEquals(Collections.emptyList(), provider.drain(queue, EMPTY_QUEUE_WAIT_MILLIS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("providers")
    void brokerSideDropFailsTheWriter(Provider provider) throws Exception {
        JmsSinkWriter writer = writer(provider, Collections.emptyMap(), null);
        writer.write(row(1));
        writer.prepareCommit(1);
        writer.write(row(2));

        provider.dropConnections();
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> commitFails(writer));
        writer.close();

        // Message 1 was committed before the drop; message 2 was not, and the engine replays it.
        assertEquals(
                Collections.singletonList(json(1)), provider.drain(queue, MESSAGE_WAIT_MILLIS));
    }

    @Test
    void textFormatJoinsFieldsWithTheDelimiter() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        Map<String, Object> options = new HashMap<>();
        options.put("format", "text");
        options.put("field_delimiter", "|");
        JmsSinkWriter writer = writer(provider, options, null);
        writer.write(row(7));
        writer.prepareCommit(1);

        assertEquals(Collections.singletonList("7|m7"), provider.drain(queue, MESSAGE_WAIT_MILLIS));
    }

    @Test
    void prepareCommitWithoutWritesOpensNoConnection() throws Exception {
        Provider provider = JmsTestBrokers.artemisCore();
        AtomicBoolean connected = new AtomicBoolean();
        ConnectionFactory real = provider.factory();
        ConnectionFactory recording =
                proxy(
                        ConnectionFactory.class,
                        (proxy, method, args) -> {
                            connected.set(true);
                            return invoke(method, real, args);
                        });
        JmsSinkWriter writer = writer(provider, Collections.emptyMap(), recording);

        writer.prepareCommit(1);
        writer.abortPrepare();
        writer.close();

        assertFalse(connected.get());
    }

    private JmsSinkWriter writer(
            Provider provider, Map<String, Object> extraOptions, ConnectionFactory factory) {
        // Ordered, so the row columns are (id, text).
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", "int");
        fields.put("text", "string");
        Map<String, Object> options = new HashMap<>();
        options.put("connection_factory_class", provider.factoryClass());
        options.put("connection_factory_properties", provider.factoryProperties());
        options.put("destination", queue);
        options.put("schema", Collections.singletonMap("fields", fields));
        options.putAll(extraOptions);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        JmsSinkFactory.validate(config);
        CatalogTable table = CatalogTableUtil.buildWithConfig(config);
        JmsSinkWriter writer =
                new JmsSinkWriter(config, JmsSink.createSerializer(config, table), factory);
        writers.add(writer);
        return writer;
    }

    private Message receiveOne(Provider provider) throws Exception {
        Connection connection = provider.factory().createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
            Message message = consumer.receive(MESSAGE_WAIT_MILLIS);
            assertTrue(message != null, "no message on " + queue);
            if (message instanceof BytesMessage) {
                ((BytesMessage) message).reset();
            }
            return message;
        } finally {
            connection.close();
        }
    }

    private static boolean commitFails(JmsSinkWriter writer) {
        try {
            writer.prepareCommit(1);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    /** Wraps a factory so that every Session#commit throws while {@code failCommits} is set. */
    private static ConnectionFactory commitFailingFactory(
            ConnectionFactory real, AtomicBoolean failCommits) {
        return proxy(
                ConnectionFactory.class,
                (factoryProxy, factoryMethod, factoryArgs) -> {
                    Object result = invoke(factoryMethod, real, factoryArgs);
                    if (!(result instanceof Connection)) {
                        return result;
                    }
                    Connection connection = (Connection) result;
                    return proxy(
                            Connection.class,
                            (connectionProxy, connectionMethod, connectionArgs) -> {
                                Object created =
                                        invoke(connectionMethod, connection, connectionArgs);
                                if (!(created instanceof Session)) {
                                    return created;
                                }
                                Session session = (Session) created;
                                return proxy(
                                        Session.class,
                                        (sessionProxy, sessionMethod, sessionArgs) -> {
                                            if (sessionMethod.getName().equals("commit")
                                                    && failCommits.get()) {
                                                throw new JMSException("injected commit failure");
                                            }
                                            return invoke(sessionMethod, session, sessionArgs);
                                        });
                            });
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T)
                Proxy.newProxyInstance(
                        JmsSinkWriterTest.class.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private static SeaTunnelRow row(int id) {
        return new SeaTunnelRow(new Object[] {id, "m" + id});
    }

    private static String json(int id) {
        return "{\"id\":" + id + ",\"text\":\"m" + id + "\"}";
    }
}
