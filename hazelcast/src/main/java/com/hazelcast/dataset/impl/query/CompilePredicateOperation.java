package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;

import java.io.IOException;

import static com.hazelcast.dataset.impl.DataSetDataSerializerHook.COMPILE_PREDICATE_OPERATION;

public class CompilePredicateOperation extends DataSetOperation {

    public Predicate predicate;
    private String compileId;

    public CompilePredicateOperation() {
    }

    public CompilePredicateOperation(String name, String compileId, Predicate predicate) {
        super(name);
        this.predicate = predicate;
        this.compileId = compileId;
    }

    @Override
    public int getId() {
        return COMPILE_PREDICATE_OPERATION;
    }

    @Override
    public void run() throws Exception {
        partition.compilePredicate(compileId, predicate);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(compileId);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        compileId = in.readUTF();
        predicate = in.readObject();
    }
}
