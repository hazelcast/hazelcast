package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;

import java.io.IOException;

public class DeregistrationOperation extends AbstractOperation {

    private String topic;
    private String id;

    public DeregistrationOperation() {
    }

    public DeregistrationOperation(String topic, String id) {
        this.topic = topic;
        this.id = id;
    }

    @Override
    public void run() throws Exception {
        EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
        EventServiceSegment segment = eventService.getSegment(getServiceName(), false);
        if (segment != null) {
            segment.removeRegistration(topic, id);
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(topic);
        out.writeUTF(id);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        topic = in.readUTF();
        id = in.readUTF();
    }
}
