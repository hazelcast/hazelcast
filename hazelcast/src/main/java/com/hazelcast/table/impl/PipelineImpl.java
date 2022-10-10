package com.hazelcast.table.impl;
// todo: we don't need a IOBuffer for all the requests. We should just add to an existing IOBuffer.

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.alto.AltoRuntime;
import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.alto.OpCodes;
import com.hazelcast.internal.alto.PartitionActorRef;
import com.hazelcast.internal.alto.RequestFuture;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.table.Pipeline;

import java.util.Arrays;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;

/**
 * todo:
 * can we collect the requests into a single IOBuffer and then flush that buffer in 1 go
 * <p>
 * And when processing such a pipeline, we process them all and send the results in 1 io bufer
 * <p>
 * And when receiving the response, we wait for all responses to be received and then notify once.
 */
public final class PipelineImpl implements Pipeline {

    private final AltoRuntime altoRuntime;
    private final IOBufferAllocator requestAllocator;
    private final int partitionCount;
    private PartitionActorRef actorRef;
    private final InternalPartitionServiceImpl partitionService;
    private int partitionId = -1;
    private IOBuffer request;
    private int countPos;
    private int count;

    public PipelineImpl(AltoRuntime altoRuntime, IOBufferAllocator requestAllocator) {
        this.altoRuntime = altoRuntime;
        this.requestAllocator = requestAllocator;
        this.partitionService = altoRuntime.node.partitionService;
        this.partitionCount = altoRuntime.node.nodeEngine.getPartitionService().getPartitionCount();
        this.request = new IOBuffer(64 * 1024);//requestAllocator.allocate();
    }

    public void noop(int partitionId) {
        init(partitionId);

        int sizePos = request.position();
        // size placeholder
        request.writeInt(0);
        // opcode
        request.writeInt(OpCodes.NOOP);
        // set the size.
        request.putInt(sizePos, request.position() - sizePos);

        count++;
    }

    @Override
    public void get(byte[] key) {
        init(hashToIndex(Arrays.hashCode(key), partitionCount));

        int sizePos = request.position();
        // size placeholder
        request.writeInt(0);
        // opcode
        request.writeInt(OpCodes.GET);
        // writing the key
        request.writeSizedBytes(key);
        // fixing the size
        request.putInt(sizePos, request.position() - sizePos);

        count++;
    }

    @Override
    public void set(byte[] key, byte[] value) {
        init(hashToIndex(Arrays.hashCode(key), partitionCount));

        int sizePos = request.position();
        // size placeholder
        request.writeInt(0);
        // opcode
        request.writeInt(OpCodes.SET);
        // writing the key
        request.writeSizedBytes(key);
        // writing the key
        request.writeSizedBytes(value);
        // fixing the size
        request.putInt(sizePos, request.position() - sizePos);

        count++;
    }

    private void init(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("PartitionId can't be smaller than 0");
        }

        if (partitionId > partitionCount - 1) {
            throw new IllegalArgumentException("PartitionId can't be larger than " + (partitionCount - 1) + " but was:" + partitionId);
        }

        if (this.partitionId == -1) {
            Address address = partitionService.getPartitionOwner(partitionId);
            if (address == null) {
                throw new RuntimeException("Address is still null (we need to deal with this situation better)");
            }

            this.partitionId = partitionId;
            this.actorRef = altoRuntime.partitionActorRefs()[partitionId];
            FrameCodec.writeRequestHeader(request, partitionId, OpCodes.PIPELINE);
            countPos = request.position();
            request.writeInt(0);
        } else if (partitionId != this.partitionId) {
            throw new RuntimeException("Cross partition request detected; expected "
                    + this.partitionId + " found: " + partitionId);
        }
    }

    @Override
    public void execute() {
        request.putInt(countPos, count);
        FrameCodec.setSize(request);

        RequestFuture<IOBuffer> requestFuture = actorRef.submit(request);
        IOBuffer response = requestFuture.join();
        response.release();
    }

    @Override
    public void reset() {
        partitionId = -1;
        //this.request = requestAllocator.allocate();
        request.clear();
        count = 0;
    }
}