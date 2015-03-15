package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.eventservice.impl.EventPacket;
import com.hazelcast.spi.impl.eventservice.impl.EventPacketProcessor;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;

import java.io.IOException;

public class SendEventOperation extends AbstractOperation {
    private EventPacket eventPacket;
    private int orderKey;

    public SendEventOperation() {
    }

    public SendEventOperation(EventPacket eventPacket, int orderKey) {
        this.eventPacket = eventPacket;
        this.orderKey = orderKey;
    }

    @Override
    public void run() throws Exception {
        EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
        eventService.executeEventCallback(new EventPacketProcessor(eventService, eventPacket, orderKey));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        eventPacket.writeData(out);
        out.writeInt(orderKey);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        eventPacket = new EventPacket();
        eventPacket.readData(in);
        orderKey = in.readInt();
    }
}
