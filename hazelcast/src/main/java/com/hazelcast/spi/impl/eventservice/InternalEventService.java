package com.hazelcast.spi.impl.eventservice;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;

/**
 * The InternalEventService is an {@link EventService} interface that adds additional capabilities
 * we don't want to expose to the end user. So they are purely meant to be used internally.
 */
public interface InternalEventService extends EventService {

    /**
     * Handles an event-packet.
     *
     * @param packet the event packet to handle.
     */
    void handleEvent(Packet packet);

    /**
     * Closes an EventRegistration.
     *
     * If the EventRegistration has any closeable resource, e.g. a listener, than this listener is closed.
     *
     * @param eventRegistration the EventRegistration to close.
     */
    void close(EventRegistration eventRegistration);
}
