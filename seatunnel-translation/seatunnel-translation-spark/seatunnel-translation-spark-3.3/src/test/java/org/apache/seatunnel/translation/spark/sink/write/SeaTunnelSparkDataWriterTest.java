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

package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@SuppressWarnings("unchecked")
class SeaTunnelSparkDataWriterTest {

    private SinkWriter<SeaTunnelRow, Object, Object> sinkWriter;
    private SinkCommitter<Object> sinkCommitter;
    private EventListener eventListener;
    private SinkWriter.Context context;

    @BeforeEach
    void setUp() throws IOException {
        sinkWriter = mock(SinkWriter.class);
        when(sinkWriter.prepareCommit(anyLong())).thenReturn(Optional.empty());
        sinkCommitter = mock(SinkCommitter.class);
        eventListener = mock(EventListener.class);
        context = mock(SinkWriter.Context.class);
        when(context.getEventListener()).thenReturn(eventListener);
    }

    private SeaTunnelSparkDataWriter<Object, Object> newWriter() {
        return new SeaTunnelSparkDataWriter<>(sinkWriter, sinkCommitter, null, 1L, context);
    }

    @Test
    void abortClosesTheWriterAfterRollingBack() throws IOException {
        newWriter().abort();

        InOrder order = inOrder(sinkWriter, sinkCommitter, eventListener);
        order.verify(sinkWriter).abortPrepare();
        order.verify(sinkCommitter).abort(Collections.emptyList());
        order.verify(sinkWriter).close();
        order.verify(eventListener).onEvent(any(WriterCloseEvent.class));
    }

    @Test
    void abortClosesTheSharedResources() throws Exception {
        MultiTableResourceManager<Object> resourceManager = mock(MultiTableResourceManager.class);
        sinkWriter =
                mock(SinkWriter.class, withSettings().extraInterfaces(SupportResourceShare.class));
        when(((SupportResourceShare<Object>) sinkWriter)
                        .initMultiTableResourceManager(anyInt(), anyInt()))
                .thenReturn(resourceManager);

        newWriter().abort();

        verify(sinkWriter).close();
        verify(resourceManager).close();
    }

    @Test
    void abortClosesTheWriterWhenTheRollbackFails() throws IOException {
        IllegalStateException rollbackFailure = new IllegalStateException("rollback failed");
        doThrow(rollbackFailure).when(sinkWriter).abortPrepare();

        SeaTunnelSparkDataWriter<Object, Object> writer = newWriter();

        Assertions.assertSame(
                rollbackFailure,
                Assertions.assertThrows(IllegalStateException.class, writer::abort));
        verify(sinkWriter).close();
    }

    @Test
    void aFailedCloseDoesNotFailTheAbort() throws IOException {
        MultiTableResourceManager<Object> resourceManager = mock(MultiTableResourceManager.class);
        sinkWriter =
                mock(SinkWriter.class, withSettings().extraInterfaces(SupportResourceShare.class));
        when(((SupportResourceShare<Object>) sinkWriter)
                        .initMultiTableResourceManager(anyInt(), anyInt()))
                .thenReturn(resourceManager);
        doThrow(new IOException("close failed")).when(sinkWriter).close();

        Assertions.assertDoesNotThrow(newWriter()::abort);

        verify(resourceManager).close();
        verify(eventListener, never()).onEvent(any());
    }

    @Test
    void abortAfterAFailedCommitDoesNotCloseAgain() throws IOException {
        doThrow(new IOException("close failed")).when(sinkWriter).close();
        when(sinkWriter.prepareCommit(anyLong())).thenReturn(Optional.empty());
        SeaTunnelSparkDataWriter<Object, Object> writer = newWriter();

        Assertions.assertThrows(IOException.class, writer::commit);
        writer.abort();

        verify(sinkWriter, times(1)).close();
    }

    @Test
    void commitClosesTheWriterOnce() throws IOException {
        SeaTunnelSparkDataWriter<Object, Object> writer = newWriter();

        writer.commit();

        InOrder order = inOrder(sinkWriter, eventListener);
        order.verify(sinkWriter).close();
        order.verify(eventListener).onEvent(any(WriterCloseEvent.class));
    }
}
