package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.CallStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.spi.CallStatus.DONE_RESPONSE;

public class ExecuteAggregationOperation extends DataSetOperation {

    private Map<String, Object> bindings;
    private String preparationId;
    private boolean forkJoin;
    private transient Aggregator response;


    public ExecuteAggregationOperation() {
    }

    public ExecuteAggregationOperation(String name, String preparationId, Map<String, Object> bindings, boolean forkJoin) {
        super(name);
        this.preparationId = preparationId;
        this.bindings = bindings;
        this.forkJoin = forkJoin;
    }

    @Override
    public CallStatus call() throws Exception {
        if (forkJoin) {
            AggregateFJResult result = partition.executeAggregateFJ(preparationId, bindings);
            //todo: we don't want to wait for completion on the operation thread
            result.aggregator.combine(result.task.join());
            response = result.aggregator;
            return DONE_RESPONSE;
        } else {
            response = partition.executeAggregationPartitionThread(preparationId, bindings);
            return DONE_RESPONSE;
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.EXECUTE_AGGREGATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeBoolean(forkJoin);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        preparationId = in.readUTF();
        forkJoin = in.readBoolean();
        int size = in.readInt();
        bindings = new HashMap<>(size);
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
