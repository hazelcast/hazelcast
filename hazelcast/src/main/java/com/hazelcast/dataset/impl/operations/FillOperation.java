package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.function.Supplier;

import java.io.IOException;

public class FillOperation extends DataSetOperation {

    private long count;
    private Supplier supplier;

    public FillOperation() {
    }

    public FillOperation(String name, Supplier supplier, long count) {
        super(name);
        this.supplier = supplier;
        this.count = count;
    }

    @Override
    public void run() {
        getLogger().info("Executing fill operation:" + getPartitionId() + " count:" + count);

        for (long k = 0; k < count; k++) {
            partition.insert(null, supplier.get());
        }
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.FILL_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(count);
        out.writeObject(supplier);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        count = in.readLong();
        supplier = in.readObject();
    }
}
