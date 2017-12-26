package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;

import java.io.IOException;

import static com.hazelcast.dataset.impl.DataSetDataSerializerHook.PREPARE_QUERY_OPERATION;

public class PrepareQueryOperation extends DataSetOperation {

    public Predicate predicate;
    private String preparationId;

    public PrepareQueryOperation() {
    }

    public PrepareQueryOperation(String name, String preparationId, Predicate predicate) {
        super(name);
        this.predicate = predicate;
        this.preparationId = preparationId;
    }

    @Override
    public int getId() {
        return PREPARE_QUERY_OPERATION;
    }

    @Override
    public void run() throws Exception {
        partition.prepareQuery(preparationId, predicate);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        preparationId = in.readUTF();
        predicate = in.readObject();
    }
}
