package com.hazelcast.queue.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;

import java.io.IOException;

/**
 * @ali 5/9/13
 */
public class AddListenerRequest extends CallableClientRequest implements Portable {

    private String name;
    private boolean includeValue;

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
                    clientEngine.sendResponse(endpoint, event.toString());
                }
                else {
//                    System.err.println("De-registering listener for " + name);
//                    service.removeItemListener(name, this);
                }
            }
        };
        service.addItemListener(name, listener, includeValue);
        return null;
    }
}
