package com.hazelcast.management;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.EventService;
import java.io.IOException;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.EventServiceMBean}.
 */
public class SerializableEventServiceBean implements DataSerializable {

    private int eventThreadCount;
    private int eventQueueCapacity;
    private int eventQueueSize;

    public SerializableEventServiceBean() {
    }

    public SerializableEventServiceBean(EventService es) {
        this.eventThreadCount = es.getEventThreadCount();
        this.eventQueueCapacity = es.getEventQueueCapacity();
        this.eventQueueSize = es.getEventQueueSize();
    }

    public int getEventThreadCount() {
        return eventThreadCount;
    }

    public void setEventThreadCount(int eventThreadCount) {
        this.eventThreadCount = eventThreadCount;
    }

    public int getEventQueueCapacity() {
        return eventQueueCapacity;
    }

    public void setEventQueueCapacity(int eventQueueCapacity) {
        this.eventQueueCapacity = eventQueueCapacity;
    }

    public int getEventQueueSize() {
        return eventQueueSize;
    }

    public void setEventQueueSize(int eventQueueSize) {
        this.eventQueueSize = eventQueueSize;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(eventThreadCount);
        out.writeInt(eventQueueCapacity);
        out.writeInt(eventQueueSize);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventThreadCount = in.readInt();
        eventQueueCapacity = in.readInt();
        eventQueueSize = in.readInt();
    }
}
