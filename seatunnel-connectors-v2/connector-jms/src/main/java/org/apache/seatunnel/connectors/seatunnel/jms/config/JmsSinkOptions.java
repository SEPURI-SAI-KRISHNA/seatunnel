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

package org.apache.seatunnel.connectors.seatunnel.jms.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/** Options of the JMS queue sink. */
public class JmsSinkOptions extends JmsBaseOptions {

    public enum MessageFormat {
        JSON,
        TEXT
    }

    public enum MessageType {
        TEXT,
        BYTES
    }

    public enum DeliveryMode {
        PERSISTENT,
        NON_PERSISTENT
    }

    public static final Option<MessageFormat> FORMAT =
            Options.key("format")
                    .enumType(MessageFormat.class)
                    .defaultValue(MessageFormat.JSON)
                    .withDescription("Format of the message body: JSON or delimited TEXT.");

    public static final Option<String> FIELD_DELIMITER =
            Options.key("field_delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("Field delimiter for TEXT bodies.");

    public static final Option<MessageType> MESSAGE_TYPE =
            Options.key("message_type")
                    .enumType(MessageType.class)
                    .defaultValue(MessageType.TEXT)
                    .withDescription(
                            "JMS message type: TEXT sends a TextMessage, BYTES sends a "
                                    + "BytesMessage holding the UTF-8 body.");

    public static final Option<DeliveryMode> DELIVERY_MODE =
            Options.key("delivery_mode")
                    .enumType(DeliveryMode.class)
                    .defaultValue(DeliveryMode.PERSISTENT)
                    .withDescription("JMS delivery mode: PERSISTENT or NON_PERSISTENT.");

    public static final Option<Integer> PRIORITY =
            Options.key("priority")
                    .intType()
                    .defaultValue(4)
                    .withDescription("JMS priority of the messages, from 0 to 9.");

    public static final Option<Long> TIME_TO_LIVE =
            Options.key("time_to_live")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "Time to live of the messages in milliseconds; 0 means they never "
                                    + "expire.");

    public static final Option<Integer> MAX_UNCOMMITTED_MESSAGES =
            Options.key("max_uncommitted_messages")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum number of messages a writer sends in one transaction. "
                                    + "The writer commits when it reaches this number and "
                                    + "before every checkpoint. Keep it below any broker limit "
                                    + "on uncommitted messages per transaction.");
}
