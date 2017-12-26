package com.hazelcast.dataseries.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.dataseries.impl.operations.DataSeriesOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class FetchAggregateOperation extends DataSeriesOperation {

    private String aggregatorId;
    private transient Aggregator response;

    public FetchAggregateOperation() {
    }

    public FetchAggregateOperation(String name, String aggregatorId) {
        super(name);
        this.aggregatorId = aggregatorId;
    }

    @Override
    public void run() throws Exception {
        response = partition.fetchAggregate(aggregatorId);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.FETCH_AGGREGATOR_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTF(aggregatorId);

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        aggregatorId = in.readUTF();
    }
}
