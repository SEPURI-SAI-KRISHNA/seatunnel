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
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jms.client.JmsConnectionFactories;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.api.configuration.util.Conditions.greaterOrEqual;
import static org.apache.seatunnel.api.configuration.util.Conditions.greaterThan;
import static org.apache.seatunnel.api.configuration.util.Conditions.lessOrEqual;
import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.CONNECTION_FACTORY_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.CONNECTION_FACTORY_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.DELIVERY_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.DESTINATION;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.JNDI_CONNECTION_FACTORY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.JNDI_INITIAL_CONTEXT_FACTORY;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.JNDI_PROPERTIES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.JNDI_PROVIDER_URL;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.MAX_UNCOMMITTED_MESSAGES;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.MESSAGE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.PRIORITY;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.TIME_TO_LIVE;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.USERNAME;

/** Factory of the JMS queue sink. */
@AutoService(Factory.class)
public class JmsSinkFactory implements TableSinkFactory {

    static final String IDENTIFIER = "Jms";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .exclusive(CONNECTION_FACTORY_CLASS, JNDI_CONNECTION_FACTORY_NAME)
                .required(DESTINATION, notBlank(DESTINATION))
                .optional(
                        CONNECTION_FACTORY_PROPERTIES,
                        JNDI_INITIAL_CONTEXT_FACTORY,
                        JNDI_PROVIDER_URL,
                        JNDI_PROPERTIES)
                .bundled(USERNAME, PASSWORD)
                .optional(FORMAT, MESSAGE_TYPE, DELIVERY_MODE)
                .optional(FIELD_DELIMITER, notBlank(FIELD_DELIMITER))
                .optional(PRIORITY, greaterOrEqual(PRIORITY, 0), lessOrEqual(PRIORITY, 9))
                .optional(TIME_TO_LIVE, greaterOrEqual(TIME_TO_LIVE, 0L))
                .optional(MAX_UNCOMMITTED_MESSAGES, greaterThan(MAX_UNCOMMITTED_MESSAGES, 0))
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        validate(config);
        return () -> new JmsSink(config, context.getCatalogTable());
    }

    /** Validates the options before any connection is opened. */
    static void validate(ReadonlyConfig config) {
        ConfigValidator.of(config).validate(new JmsSinkFactory().optionRule());
        JmsConnectionFactories.checkFactoryOptions(config);
    }
}
