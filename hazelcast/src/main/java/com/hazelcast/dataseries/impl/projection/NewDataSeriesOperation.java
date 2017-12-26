package com.hazelcast.dataseries.impl.projection;

import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.dataseries.impl.Partition;
import com.hazelcast.dataseries.impl.operations.DataSeriesOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NewDataSeriesOperation extends DataSeriesOperation {

    private Map<String, Object> bindings;
    private String dstName;
    private String preparationId;
    private String recordType;

    public NewDataSeriesOperation() {
    }

    public NewDataSeriesOperation(String srcName,
                                  String dstName,
                                  String recordType,
                                  String preparationId,
                                  Map<String, Object> bindings) {
        super(srcName);
        this.dstName = dstName;
        this.recordType = recordType;
        this.preparationId = preparationId;
        this.bindings = bindings;
    }

    @Override
    public void run() throws Exception {
        Partition srcPartition = partition;
        DataSeriesConfig srcConfig = srcPartition.getConfig();

        // the dst configuration isn't optimal. But will do for now (large enough the hold the original payload)
        DataSeriesConfig dstConfig = new DataSeriesConfig(dstName)
                .setValueClass(NewDataSeriesOperation.class.getClassLoader().loadClass(recordType))
                .setInitialSegmentSize(srcConfig.getMaxSegmentSize())
                .setMaxSegmentSize(srcConfig.getMaxSegmentSize())
                .setSegmentsPerPartition(Integer.MAX_VALUE);

        // in theory the size can be calculated based on record count + projection-record size
        Partition dstPartition = dataSeriesService
                .getDataSeriesContainer(dstName, dstConfig)
                .getPartition(getPartitionId());

        srcPartition.executeProjectionPartitionThread(preparationId, bindings, o -> dstPartition.insert(null, o));
        dstPartition.freeze();
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.NEW_DATASERIES_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(dstName);
        out.writeUTF(recordType);
        out.writeUTF(preparationId);
        out.writeInt(bindings.size());
        for (Map.Entry<String, Object> entry : bindings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dstName = in.readUTF();
        recordType = in.readUTF();
        preparationId = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<>(size);
        for (int k = 0; k < size; k++) {
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}