package com.hazelcast.cardinality.hyperloglog.operations;

import com.hazelcast.cardinality.hyperloglog.HyperLogLogDataSerializerHook;
import com.hazelcast.cardinality.hyperloglog.struct.IHyperLogLog;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class AddHashAndEstimateCardinalityOperation
        extends AbstractHyperLogLogOperation {

    private long hash;
    private long estimate;

    public AddHashAndEstimateCardinalityOperation() {}

    public AddHashAndEstimateCardinalityOperation(String name, long hash) {
        super(name);
        this.hash = hash;
    }

    @Override
    public int getId() {
        return HyperLogLogDataSerializerHook.ADD_HASH_AND_ESTIMATE;
    }

    @Override
    public void run() throws Exception {
        IHyperLogLog hll = getHyperLogLogContainer();
        hll.aggregate(hash);
        estimate = hll.estimate();
    }

    @Override
    public Object getResponse() {
        return estimate;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(hash);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        hash = in.readLong();
    }
}
