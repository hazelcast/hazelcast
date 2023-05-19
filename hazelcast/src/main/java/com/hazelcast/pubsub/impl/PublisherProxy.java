/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.pubsub.Publisher;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_CRC32;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.pubsub.impl.CRC.crc32;
import static com.hazelcast.pubsub.impl.PublishCmd.ID;

@SuppressWarnings("checkstyle:MagicNumber")
public class PublisherProxy extends AbstractDistributedObject implements Publisher {

    private final ConcurrentIOBufferAllocator requestAllocator;
    private final byte[] topicNameBytes;
    private final TpcRuntime tpcRuntime;
    private final int requestTimeoutMs;
    private final String topicName;
    private final int partitionCount;

    public PublisherProxy(NodeEngineImpl nodeEngine, PublisherService publisherService, String name) {
        super(nodeEngine, publisherService);
        this.topicName = name;
        this.tpcRuntime = nodeEngine.getNode().getTpcRuntime();
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.topicNameBytes = name.getBytes();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
    }

    @Override
    public String getName() {
        return topicName;
    }

    @Override
    public String getServiceName() {
        return PublisherService.SERVICE_NAME;
    }

    @Override
    public void publish(byte[] message, byte syncOption) {
        publish(ThreadLocalRandom.current().nextInt(partitionCount), message, syncOption);
    }

    @Override
    public void publish(int partitionId, byte[] message, byte syncOption) {
        // todo: currently there is no batching

        IOBuffer request = requestAllocator.allocate();
        FrameCodec.writeRequestHeader(request, partitionId, ID);

        request.writeSizePrefixedBytes(topicNameBytes);

        request.writeByte(syncOption);

        // number of messages
        request.writeInt(1);

        // the total size of all the messages
        request.writeInt(SIZEOF_INT + message.length + SIZEOF_CRC32);

        // and now the individual messages. For the time being there is just one
        request.writeSizePrefixedBytes(message);

        request.writeInt(crc32(message));

        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> future = tpcRuntime.getRpcCore().invoke(partitionId, request);
        try {
            IOBuffer response = future.get();
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
