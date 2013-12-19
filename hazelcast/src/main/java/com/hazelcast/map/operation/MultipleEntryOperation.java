package com.hazelcast.map.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.*;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * date: 19/12/13
 * author: eminn
 */
public class MultipleEntryOperation extends AbstractMapOperation implements BackupAwareOperation, PartitionAwareOperation {

    private static final EntryEventType __NO_NEED_TO_FIRE_EVENT = null;
    private EntryProcessor entryProcessor;
    private Set<Data> keys;
    MapEntrySet response;


    public MultipleEntryOperation(){
    }

    public MultipleEntryOperation(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        super(name);
        this.keys = keys;
        this.entryProcessor = entryProcessor;
    }

    public void innerBeforeRun() {
        final ManagedContext managedContext = getNodeEngine().getSerializationService().getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    public void run() throws Exception {
        response = new MapEntrySet();
        final PartitionService partitionService = getNodeEngine().getPartitionService();
        final RecordStore recordStore = mapService.getRecordStore(getPartitionId(),name);
        MapEntrySimple entry;

        for(Data key:keys)
        {
            if(partitionService.getPartitionId(key) != getPartitionId())
                continue;
            Object objectKey = mapService.toObject(key);
            final Map.Entry<Data, Object> mapEntry = recordStore.getMapEntry(key);
            final Object valueBeforeProcess = mapService.toObject(mapEntry.getValue());
            entry = new MapEntrySimple(objectKey,valueBeforeProcess);
            final Object result = entryProcessor.process(entry);
            final Object valueAfterProcess = entry.getValue();
            Data dataValue = null;
            if (result != null) {
                dataValue = mapService.toData(result);
                response.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, dataValue));
            }
            EntryEventType eventType;
            if (valueAfterProcess == null) {
                recordStore.remove(key);
                eventType = EntryEventType.REMOVED;
            } else {
                if (valueBeforeProcess == null) {
                    eventType = EntryEventType.ADDED;
                }
                // take this case as a read so no need to fire an event.
                else if (!entry.isModified()) {
                    eventType = __NO_NEED_TO_FIRE_EVENT;
                } else {
                    eventType = EntryEventType.UPDATED;
                }
                // todo if this is a read only operation, record access operations should be done.
                if (eventType != __NO_NEED_TO_FIRE_EVENT) {
                    recordStore.put(new AbstractMap.SimpleImmutableEntry<Data, Object>(key, valueAfterProcess));
                }
            }

            if (eventType != __NO_NEED_TO_FIRE_EVENT) {
                mapService.publishEvent(getCallerAddress(), name, eventType, key, (Data)mapEntry.getValue(), dataValue);
                if (mapContainer.isNearCacheEnabled()
                        && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange()) {
                    mapService.invalidateAllNearCaches(name, key);
                }
                if (mapContainer.getWanReplicationPublisher() != null && mapContainer.getWanMergePolicy() != null) {
                    if (EntryEventType.REMOVED.equals(eventType)) {
                        mapService.publishWanReplicationRemove(name, key, Clock.currentTimeMillis());
                    } else {
                        Record r = recordStore.getRecord(key);
                        SimpleEntryView entryView = new SimpleEntryView(key, mapService.toData(dataValue), r.getStatistics(), r.getVersion());
                        mapService.publishWanReplicationUpdate(name, entryView);
                    }
                }
            }

        }

    }
    public void afterRun() throws Exception {
        super.afterRun();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }



    @Override
    public String toString() {
        return "MultipleEntryOperation{}";
    }

    @Override
    public boolean shouldBackup() {
        return entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new MultipleEntryBackupOperation(name,keys, backupProcessor) : null;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
        int size = in.readInt();
        keys = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data key = new Data();
            key.readData(in);
            keys.add(key);
        }

    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
        out.writeInt(keys.size());
        for(Data key:keys)
        {
            key.writeData(out);
        }

    }


}
