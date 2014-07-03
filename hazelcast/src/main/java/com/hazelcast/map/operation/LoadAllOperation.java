package com.hazelcast.map.operation;

import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Triggers map store load of all given keys.
 */
public class LoadAllOperation extends AbstractMapOperation implements PartitionAwareOperation {

    private List<Data> keys;

    private boolean replaceExistingValues;

    public LoadAllOperation() {
        keys = Collections.emptyList();
    }

    public LoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public void run() throws Exception {
        final int partitionId = getPartitionId();
        final RecordStore recordStore = mapService.getMapServiceContext().getRecordStore(partitionId, name);
        keys = selectThisPartitionsKeys(this.keys);
        recordStore.loadAllFromStore(keys, replaceExistingValues);
    }

    private List<Data> selectThisPartitionsKeys(Collection<Data> keys) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final InternalPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        final int partitionId = getPartitionId();
        List<Data> dataKeys = null;
        for (Data key : keys) {
            if (partitionId == partitionService.getPartitionId(key)) {
                if (dataKeys == null) {
                    dataKeys = new ArrayList<Data>();
                }
                dataKeys.add(key);
            }
        }
        if (dataKeys == null) {
            return Collections.emptyList();
        }
        return dataKeys;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final int size = keys.size();
        out.writeInt(size);
        for (Data key : keys) {
            key.writeData(out);
        }
        out.writeBoolean(replaceExistingValues);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size > 0) {
            keys = new ArrayList<Data>(size);
        }
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            keys.add(data);
        }
        replaceExistingValues = in.readBoolean();
    }
}
