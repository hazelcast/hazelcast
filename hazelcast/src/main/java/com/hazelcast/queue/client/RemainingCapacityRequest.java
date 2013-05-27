package com.hazelcast.queue.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueueContainer;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;

import java.io.IOException;

/**
 * @ali 5/23/13
 */
public class RemainingCapacityRequest extends CallableClientRequest implements Portable, RetryableRequest {

    protected String name;

    public RemainingCapacityRequest() {
    }

    public RemainingCapacityRequest(String name) {
        this.name = name;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.REMAINING_CAPACITY;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    public Object call() throws Exception {
        QueueService service = getService();
        QueueContainer container = service.getOrCreateContainer(name, false);
        return container.getConfig().getMaxSize() - container.size();
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }
}
