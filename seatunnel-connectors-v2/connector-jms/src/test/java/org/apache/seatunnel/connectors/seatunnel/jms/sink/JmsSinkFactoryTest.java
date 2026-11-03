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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JmsSinkFactoryTest {

    @Test
    void factoryIdentifierIsJms() {
        assertEquals("Jms", new JmsSinkFactory().factoryIdentifier());
    }

    @Test
    void minimalConfigurationIsValid() {
        assertDoesNotThrow(() -> validate(config()));
    }

    @Test
    void destinationAndOneConnectionFactoryModeAreRequired() {
        Map<String, Object> noDestination = config();
        noDestination.remove("destination");
        assertThrows(OptionValidationException.class, () -> validate(noDestination));

        Map<String, Object> bothModes = config();
        bothModes.put("jndi_connection_factory_name", "ConnectionFactory");
        assertThrows(OptionValidationException.class, () -> validate(bothModes));

        Map<String, Object> jndiWithoutContextFactory = config();
        jndiWithoutContextFactory.remove("connection_factory_class");
        jndiWithoutContextFactory.remove("connection_factory_properties");
        jndiWithoutContextFactory.put("jndi_connection_factory_name", "ConnectionFactory");
        assertThrows(IllegalArgumentException.class, () -> validate(jndiWithoutContextFactory));
    }

    @Test
    void producerSettingsAreBounded() {
        Map<String, Object> priorityTooHigh = config();
        priorityTooHigh.put("priority", 10);
        assertThrows(OptionValidationException.class, () -> validate(priorityTooHigh));

        Map<String, Object> negativePriority = config();
        negativePriority.put("priority", -1);
        assertThrows(OptionValidationException.class, () -> validate(negativePriority));

        Map<String, Object> negativeTimeToLive = config();
        negativeTimeToLive.put("time_to_live", -1L);
        assertThrows(OptionValidationException.class, () -> validate(negativeTimeToLive));

        Map<String, Object> zeroBatch = config();
        zeroBatch.put("max_uncommitted_messages", 0);
        assertThrows(OptionValidationException.class, () -> validate(zeroBatch));
    }

    @Test
    void everyOptionIsDeclaredInTheRule() {
        Map<String, Object> config = config();
        config.put("username", "writer");
        config.put("password", "secret");
        config.put("format", "text");
        config.put("field_delimiter", "|");
        config.put("message_type", "BYTES");
        config.put("delivery_mode", "NON_PERSISTENT");
        config.put("priority", 9);
        config.put("time_to_live", 60000L);
        config.put("max_uncommitted_messages", 10);
        assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config),
                                new JmsSinkFactory().optionRule(),
                                "Jms"));
    }

    private static void validate(Map<String, Object> config) {
        JmsSinkFactory.validate(ReadonlyConfig.fromMap(config));
    }

    private static Map<String, Object> config() {
        Map<String, Object> config = new HashMap<>();
        config.put("connection_factory_class", "org.apache.activemq.ActiveMQConnectionFactory");
        config.put(
                "connection_factory_properties",
                Collections.singletonMap("brokerURL", "tcp://127.0.0.1:61616"));
        config.put("destination", "orders");
        return config;
    }
}
