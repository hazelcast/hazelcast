package com.hazelcast.table.impl;

import com.hazelcast.internal.alto.Op;
import com.hazelcast.internal.alto.OpCodes;

public class PipelineOp extends Op {

    public PipelineOp() {
        super(OpCodes.PIPELINE);
    }

    @Override
    public int run() throws Exception {
        throw new UnsupportedOperationException();
    }
}
