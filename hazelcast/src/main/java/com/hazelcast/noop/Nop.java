package com.hazelcast.noop;

import com.hazelcast.core.Command;
import com.hazelcast.htable.Pipeline;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.PipelineImpl;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.noop.impl.NopCmd;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.noop.impl.NopCmd.ID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Nop implements Command {

    private final TpcRuntime tpcRuntime;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;

    public Nop(TpcRuntime tpcRuntime) {
        this.tpcRuntime = tpcRuntime;
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
    }

    public void execute(int partitionId) {
        CompletableFuture<IOBuffer> f = executeAsync(partitionId);
        try {
            IOBuffer response = f.get(requestTimeoutMs, MILLISECONDS);
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void pipeline(Pipeline p, int partitionId) {
        PipelineImpl pipeline = (PipelineImpl) p;

        pipeline.init(partitionId);

        int sizePos = pipeline.request.position();
        // size placeholder
        pipeline.request.writeInt(0);
        // opcode
        pipeline.request.writeInt(NopCmd.ID);
        // set the size.
        pipeline.request.putInt(sizePos, pipeline.request.position() - sizePos);

        pipeline.count++;
    }

    public CompletableFuture<IOBuffer> executeAsync(int partitionId) {
        //  ConcurrentIOBufferAllocator allocator = new ConcurrentIOBufferAllocator(1,true);
        IOBuffer request = requestAllocator.allocate(32);
        //   request.trackRelease=true;
        FrameCodec.writeRequestHeader(request, partitionId, ID);
        FrameCodec.setSize(request);
        return tpcRuntime.getRpcCore().invoke(partitionId, request);
    }
}
