package com.hazelcast.map.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Puts records to map which are loaded from map store by {@link com.hazelcast.core.IMap#loadAll}
 */
public class PutFromLoadAllOperation extends AbstractMapOperation implements PartitionAwareOperation, BackupAwareOperation {

    private List<Data> keyValueSequence;

    public PutFromLoadAllOperation() {
        keyValueSequence = Collections.emptyList();
    }

    public PutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
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
            final Data dataValue = keyValueSequence.get(i + 1);
            final Object objectValue = mapService.toObject(dataValue);
            final Object previousValue = recordStore.putFromLoad(key, objectValue);

            callAfterPutInterceptors(objectValue);
            publishEvent(key, mapService.toData(previousValue), dataValue);
        }
    }

    private void callAfterPutInterceptors(Object value) {
        mapService.interceptAfterPut(name, value);
    }

    private void publishEvent(Data key, Data previousValue, Data newValue) {
        final EntryEventType eventType = previousValue == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
        mapService.publishEvent(getCallerAddress(), name, eventType, key, previousValue, newValue);
    }


    @Override
    public void afterRun() throws Exception {
        final List<Data> keyValueSequence = this.keyValueSequence;
        if (keyValueSequence == null || keyValueSequence.isEmpty()) {
            return;
        }
        final int size = keyValueSequence.size();
        final List<Data> dataKeys = new ArrayList<Data>(size / 2);
        for (int i = 0; i < size; i += 2) {
            final Data key = keyValueSequence.get(i);
            dataKeys.add(key);
        }
        mapService.invalidateNearCache(name, dataKeys);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return true;
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
    public boolean shouldBackup() {
        return !keyValueSequence.isEmpty();
    }

    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new PutFromLoadKeysBackupOperation(name, keyValueSequence);
    }

    @Override
    public String toString() {
        return "PutFromLoadAllOperation{}";
    }
}
