package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.executor.StripedRunnable;

public class RemoteEventPacketProcessor extends EventPacketProcessor implements StripedRunnable {
    private EventServiceImpl eventService;
    private Packet packet;

    public RemoteEventPacketProcessor(EventServiceImpl eventService, Packet packet) {
        super(eventService, null, packet.getPartitionId());
        this.eventService = eventService;
        this.packet = packet;
    }

    @Override
    public void run() {
        try {
            Data data = packet.getData();
            EventPacket eventPacket = (EventPacket) eventService.nodeEngine.toObject(data);
            process(eventPacket);
        } catch (Exception e) {
            eventService.logger.warning("Error while logging processing event", e);
        }
    }
}
