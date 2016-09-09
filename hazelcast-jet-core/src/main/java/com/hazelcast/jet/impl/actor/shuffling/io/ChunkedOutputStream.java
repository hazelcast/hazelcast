/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.actor.shuffling.io;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.impl.actor.RingbufferActor;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.runtime.TaskContext;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class ChunkedOutputStream extends OutputStream {
    private static final int BUFFER_OFFSET = HeapData.DATA_OFFSET;

    private int bufferSize;
    private final int taskId;
    private final byte[] buffer;
    private final int vertexManagerId;
    private final int shufflingBytesSize;
    private final byte[] jobNameBytyes;
    private final RingbufferActor ringbufferActor;

    public ChunkedOutputStream(RingbufferActor ringbufferActor,
                               TaskContext taskContext, int vertexManagerId, int taskId) {
        this.taskId = taskId;
        this.ringbufferActor = ringbufferActor;
        this.shufflingBytesSize = taskContext.getJobContext().getJobConfig().getShufflingBatchSizeBytes();
        this.buffer = new byte[BUFFER_OFFSET + this.shufflingBytesSize];
        String jobName = taskContext.getJobContext().getName();
        NodeEngine nodeEngine = taskContext.getJobContext().getNodeEngine();
        this.jobNameBytyes = ((InternalSerializationService) nodeEngine.getSerializationService()).toBytes(jobName);
        this.vertexManagerId = vertexManagerId;
    }

    @Override
    public void write(int b) throws IOException {
        buffer[BUFFER_OFFSET + bufferSize++] = (byte) b;

        if (bufferSize >= shufflingBytesSize) {
            try {
                flushBuffer();
            } catch (Exception e) {
                throw unchecked(e);
            }
        }
    }

    private void flushBuffer() {
        try {
            if (this.bufferSize > 0) {
                byte[] buffer = new byte[BUFFER_OFFSET + this.bufferSize];
                System.arraycopy(this.buffer, 0, buffer, 0, BUFFER_OFFSET + this.bufferSize);

                JetPacket packet = new JetPacket(
                        taskId,
                        vertexManagerId,
                        jobNameBytyes,
                        buffer
                );

                packet.setHeader(JetPacket.HEADER_JET_DATA_CHUNK);

                ringbufferActor.consume(packet);
            }
        } finally {
            Arrays.fill(buffer, (byte) 0);
            bufferSize = 0;
        }
    }

    public void onOpen() {
        bufferSize = 0;
        Arrays.fill(buffer, (byte) 0);
    }

    public void flushSender() {
        flushBuffer();
    }
}
