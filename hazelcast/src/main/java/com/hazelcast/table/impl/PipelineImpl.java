package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.reactor.Frame;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;
import com.hazelcast.table.Pipeline;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_NOOP;
import static java.util.concurrent.TimeUnit.SECONDS;


public class PipelineImpl implements Pipeline {

    private final ReactorFrontEnd frontEnd;
    private List<Frame> requests = new ArrayList<>();
    private List<CompletableFuture> futures = new ArrayList<>();
    private int partitionId = -1;

    public PipelineImpl(ReactorFrontEnd frontEnd) {
        this.frontEnd = frontEnd;
    }

    public void noop(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("PartitionId can't be smaller than 0");
        }

        if (this.partitionId == -1) {
            this.partitionId = partitionId;
        } else if (partitionId != this.partitionId) {
            throw new RuntimeException("Cross partition request detected; expected " + this.partitionId + " found: " + partitionId);
        }

        //todo: usepool
        Frame request = new Frame(32)
                .newFuture()
                .writeRequestHeader(partitionId, TABLE_NOOP)
                .completeWriting();

        futures.add(request.future);
        requests.add(request);
    }

    @Override
    public void execute() {
        frontEnd.invoke(this);
    }

    public void await(){
        for(Future f: futures){
            try {
                f.get(23, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int getPartitionId() {
        return partitionId;
    }

    public List<Frame> getRequests() {
        return requests;
    }
}
