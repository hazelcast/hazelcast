package com.hazelcast.replicatedmap.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.record.ReplicatedRecordStore;

import java.io.IOException;

public class ClientReplicatedMapAddEntryListenerRequest extends AbstractReplicatedMapClientRequest {

    private Predicate predicate;
    private Object key;

    ClientReplicatedMapAddEntryListenerRequest() {
        super(null);
    }

    public ClientReplicatedMapAddEntryListenerRequest(String mapName, Predicate predicate, Object key) {
        super(mapName);
        this.predicate = predicate;
        this.key = key;
    }

    @Override
    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine clientEngine = getClientEngine();
        final ReplicatedRecordStore replicatedRecordStore = getReplicatedRecordStore();

        EntryListener<Object, Object> listener = new EntryListener<Object, Object>() {

            private void handleEvent(EntryEvent<Object, Object> event) {
                if (endpoint.live()) {
                    Data key = clientEngine.toData(event.getKey());
                    Data value = clientEngine.toData(event.getValue());
                    Data oldValue = clientEngine.toData(event.getOldValue());
                    ReplicatedMapPortableEntryEvent portableEntryEvent =
                            new ReplicatedMapPortableEntryEvent(key, value, oldValue,
                                    event.getEventType(), event.getMember().getUuid());
                    clientEngine.sendResponse(endpoint, portableEntryEvent);
                }
            }

            public void entryAdded(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryRemoved(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
                handleEvent(event);
            }
        };

        String registrationId;
        if (predicate == null) {
            registrationId = replicatedRecordStore.addEntryListener(listener, key);
        } else {
            registrationId = replicatedRecordStore.addEntryListener(listener, predicate, key);
        }
        endpoint.setListenerRegistration(ReplicatedMapService.SERVICE_NAME, getMapName(), registrationId);
        return true;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(key);
        out.writeObject(predicate);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readObject();
        predicate = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.ADD_LISTENER;
    }
}
