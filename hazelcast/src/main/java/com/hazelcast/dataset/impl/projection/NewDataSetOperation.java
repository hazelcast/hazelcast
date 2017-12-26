package com.hazelcast.dataset.impl.projection;

import com.hazelcast.config.DataSetConfig;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.function.Consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NewDataSetOperation extends DataSetOperation {

    private Map<String, Object> bindings;
    private String targetName;
    private String compileId;
    private String recordType;

    public NewDataSetOperation() {
    }

    public NewDataSetOperation(String srcName, String targetName, String recordType, String compileId, Map<String, Object> bindings) {
        super(srcName);
        this.targetName = targetName;
        this.recordType = recordType;
        this.compileId = compileId;
        this.bindings = bindings;
    }

    @Override
    public void run() throws Exception {
        Partition srcPartition = partition;
        DataSetConfig srcConfig = srcPartition.getConfig();

        DataSetConfig dstConfig = new DataSetConfig(targetName);
        dstConfig.setValueClass(NewDataSetOperation.class.getClassLoader().loadClass(recordType));
        // in theory the size can be calculated based on record count + projection-record size
        dstConfig.setInitialSegmentSize(srcConfig.getInitialSegmentSize());
        final Partition dstPartition = dataSetService.getDataSetContainer(targetName, dstConfig).getPartition(getPartitionId());

        srcPartition.projection(compileId, bindings, new Consumer() {
            @Override
            public void accept(Object o) {
                dstPartition.insert(null, o);
            }
        });
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.NEW_DATASET_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(targetName);
        out.writeUTF(recordType);
        out.writeUTF(compileId);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        targetName = in.readUTF();
        recordType = in.readUTF();
        compileId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}