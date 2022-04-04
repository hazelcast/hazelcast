package com.hazelcast.table.impl;

import com.hazelcast.spi.impl.reactor.Frame;
import com.hazelcast.spi.impl.reactor.Invocation;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;
import com.hazelcast.table.Pipeline;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_NOOP;


public class PipelineImpl implements Pipeline {

    private final ReactorFrontEnd frontEnd;
    private List<Invocation> invocations = new LinkedList<>();
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

        Invocation inv = new Invocation();
        inv.request = new Frame(32)
                .writeRequestHeader(partitionId, TABLE_NOOP)
                .completeWriting();

        invocations.add(inv);
    }

    @Override
    public void execute() {
        frontEnd.invoke(this);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public List<Invocation> getInvocations() {
        return invocations;
    }
}
