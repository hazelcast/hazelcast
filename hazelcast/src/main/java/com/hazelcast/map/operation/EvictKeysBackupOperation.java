package com.hazelcast.map.operation;

import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * User: ahmetmircik
 * Date: 11/1/13
 */
public class EvictKeysBackupOperation extends AbstractNamedOperation implements BackupOperation, DataSerializable {
    Set<Data> keys;
    MapService mapService;
    RecordStore recordStore;

    public EvictKeysBackupOperation() {
    }

    public EvictKeysBackupOperation(String name, Set<Data> keys) {
        super(name);
        this.keys = keys;
    }

    @Override
    public void beforeRun() throws Exception {
        mapService = getService();
        recordStore = mapService.getRecordStore(getPartitionId(), name);
    }

    public void run() {
        for (Data key : keys) {
            if (!recordStore.isLocked(key))
            {
                recordStore.evict(key);
            }
        }
    }

    public Set<Data> getKeys() {
        return keys;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null)
            out.writeInt(-1);
        else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if(size > -1) {
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                keys.add(data);
            }
        }
    }


    @Override
    public String toString() {
        return "EvictKeysBackupOperation{" +
                '}';
    }

}
