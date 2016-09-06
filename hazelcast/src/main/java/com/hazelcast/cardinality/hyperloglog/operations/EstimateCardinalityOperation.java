package com.hazelcast.cardinality.hyperloglog.operations;

import com.hazelcast.cardinality.hyperloglog.HyperLogLogDataSerializerHook;

public class EstimateCardinalityOperation
        extends AbstractHyperLogLogOperation {

    private long estimate;

    public EstimateCardinalityOperation() {}

    public EstimateCardinalityOperation(String name) {
        super(name);
    }

    @Override
    public int getId() {
        return HyperLogLogDataSerializerHook.EST_CARDINALITY;
    }

    @Override
    public void run() throws Exception {
        estimate = getHyperLogLogContainer().estimate();
    }

    @Override
    public Object getResponse() {
        return estimate;
    }

}
