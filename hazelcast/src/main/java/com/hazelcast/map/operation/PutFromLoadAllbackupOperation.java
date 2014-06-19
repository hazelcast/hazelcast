package com.hazelcast.map.operation;

import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Runs on backups.
 *
 * @see {@link PutFromLoadAllOperation}
 */
public class PutFromLoadAllBackupOperation extends AbstractMapOperation implements BackupOperation, DataSerializable {

    private List<Data> keyValueSequence;

    public PutFromLoadAllBackupOperation() {
        keyValueSequence = Collections.emptyList();
    }

    public PutFromLoadAllBackupOperation(String name, List<Data> keyValueSequence) {
        super(name);
        this.keyValueSequence = keyValueSequence;
    }

    @Override
    public void run() throws Exception {
        final List<Data> keyValueSequence = this.keyValueSequence;
        if (keyValueSequence == null || keyValueSequence.isEmpty()) {
            return;
        }
        final int partitionId = getPartitionId();
        final MapService mapService = this.mapService;
        final RecordStore recordStore = mapService.getRecordStore(partitionId, name);
        for (int i = 0; i < keyValueSequence.size(); i += 2) {
            final Data key = keyValueSequence.get(i);
            final Data value = keyValueSequence.get(i + 1);
            final Object object = mapService.toObject(value);
            recordStore.putFromLoad(key, object);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final List<Data> keyValueSequence = this.keyValueSequence;
        final int size = keyValueSequence.size();
        out.writeInt(size);
        for (Data data : keyValueSequence) {
            data.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size < 1) {
            keyValueSequence = Collections.emptyList();
        } else {
            final List<Data> tmpKeyValueSequence = new ArrayList<Data>(size);
            for (int i = 0; i < size; i++) {
                final Data data = new Data();
                data.readData(in);
                tmpKeyValueSequence.add(data);
            }
            keyValueSequence = tmpKeyValueSequence;
        }
    }

    @Override
    public String toString() {
        return "PutFromLoadAllBackupOperation{}";
    }
}
