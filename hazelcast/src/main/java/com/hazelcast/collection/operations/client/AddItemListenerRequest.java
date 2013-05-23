package com.hazelcast.collection.operations.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.impl.PortableItemEvent;

import java.io.IOException;

/**
 * @ali 5/23/13
 */
public class AddItemListenerRequest extends CallableClientRequest implements Portable {

    CollectionProxyId proxyId;
    Data key;
    boolean includeValue;
    private transient String registrationId;

    public AddItemListenerRequest() {
    }

    public AddItemListenerRequest(CollectionProxyId proxyId, Data key, boolean includeValue) {
        this.proxyId = proxyId;
        this.key = key;
        this.includeValue = includeValue;
    }

    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine clientEngine = getClientEngine();
        final CollectionService service = getService();

        ItemListener listener = new ItemListener() {
            public void itemAdded(ItemEvent item) {
                send(item);
            }

            public void itemRemoved(ItemEvent item) {
                send(item);
            }

            private void send(ItemEvent event){
                if (endpoint.live()){
                    Data item = clientEngine.toData(event.getItem());
                    PortableItemEvent portableItemEvent = new PortableItemEvent(item, event.getEventType(), event.getMember().getUuid());
                    clientEngine.sendResponse(endpoint, portableItemEvent);
                } else {
                    if (registrationId != null){
                        service.removeListener(proxyId.getName(), registrationId);
                    } else {
                        System.err.println("registrationId is null!!!");
                    }

                }
            }
        };
        registrationId = service.addListener(proxyId.getName(), listener, key, includeValue, false);
        return registrationId;
    }

    public String getServiceName() {
        return CollectionService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return CollectionPortableHook.F_ID;
    }

    public int getClassId() {
        return CollectionPortableHook.ADD_ITEM_LISTENER;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("i",includeValue);
        final ObjectDataOutput out = writer.getRawDataOutput();
        proxyId.writeData(out);
        IOUtil.writeNullableData(out, key);
    }

    public void readPortable(PortableReader reader) throws IOException {
        includeValue = reader.readBoolean("i");
        final ObjectDataInput in = reader.getRawDataInput();
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        key = IOUtil.readNullableData(in);

    }
}
