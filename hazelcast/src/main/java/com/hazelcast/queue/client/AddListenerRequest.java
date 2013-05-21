package com.hazelcast.queue.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;
import com.hazelcast.spi.impl.PortableItemEvent;

import java.io.IOException;

/**
 * @ali 5/9/13
 */
public class AddListenerRequest extends CallableClientRequest implements Portable {

    private String name;
    private boolean includeValue;
    private transient String registrationId;

    public AddListenerRequest() {
    }

    public AddListenerRequest(String name, boolean includeValue) {
        this.name = name;
        this.includeValue = includeValue;
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.ADD_LISTENER;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeBoolean("i",includeValue);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        includeValue = reader.readBoolean("i");
    }

    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine clientEngine = getClientEngine();
        final QueueService service = getService();

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
                        service.removeItemListener(name, registrationId);
                    } else {
                        System.err.println("registrationId is null!!!");
                    }

                }
            }
        };
        registrationId = service.addItemListener(name, listener, includeValue);
        return registrationId;
    }
}
