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
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.MessageFormat;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.jms.config.JmsSinkOptions.FORMAT;

/**
 * Sink that sends each row as one message to a JMS queue. Rows are sent in transactions that are
 * committed before every checkpoint, so delivery is at least once.
 */
public class JmsSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
    private static final long serialVersionUID = 1L;

    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;

    public JmsSink(ReadonlyConfig config, CatalogTable catalogTable) {
        JmsSinkFactory.validate(config);
        this.config = config;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return JmsSinkFactory.IDENTIFIER;
    }

    /** Creates one writer per parallel subtask; each writer opens its own connection. */
    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new JmsSinkWriter(config, createSerializer(config, catalogTable));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    static SerializationSchema createSerializer(ReadonlyConfig config, CatalogTable table) {
        SeaTunnelRowType rowType = table.getTableSchema().toPhysicalRowDataType();
        return config.get(FORMAT) == MessageFormat.JSON
                ? new JsonSerializationSchema(rowType)
                : TextSerializationSchema.builder()
                        .seaTunnelRowType(rowType)
                        .delimiter(config.get(FIELD_DELIMITER))
                        .build();
    }
}
