package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.alto.PartitionActorRef;
import com.hazelcast.internal.tpc.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.pubsub.Publisher;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.alto.OpCodes.TOPIC_PUBLISH;

public class PublisherImpl implements Publisher {

    private final PartitionActorRef[] partitionActorRefs;
    private final ConcurrentIOBufferAllocator requestAllocator;
    private final byte[] topicIdBytes;

    public PublisherImpl(String topicId, PartitionActorRef[] partitionActorRefs) {
        this.partitionActorRefs = partitionActorRefs;
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.topicIdBytes = topicId.getBytes();
    }

    @Override
    public long publish(int partitionId, byte[] message, byte syncOption) {
        IOBuffer request = requestAllocator.allocate();
        FrameCodec.writeRequestHeader(request, partitionId, TOPIC_PUBLISH);
        request.writeByte(syncOption);
        //request.writeSizedBytes(topicIdBytes);
        request.writeSizedBytes(message);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> future = partitionActorRefs[partitionId].submit(request);
        try {
            IOBuffer response = future.get();
            long sequence = response.readLong();
            response.release();
            return sequence;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
