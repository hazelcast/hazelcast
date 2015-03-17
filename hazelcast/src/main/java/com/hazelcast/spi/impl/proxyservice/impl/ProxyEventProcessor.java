package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.util.executor.StripedRunnable;

import java.util.Collection;

final class ProxyEventProcessor implements StripedRunnable {

    private final Collection<DistributedObjectListener> listeners;
    private final DistributedObjectEvent.EventType type;
    private final String serviceName;
    private final DistributedObject object;

    ProxyEventProcessor(Collection<DistributedObjectListener> listeners, DistributedObjectEvent.EventType eventType,
                        String serviceName, DistributedObject object) {
        this.listeners = listeners;
        this.type = eventType;
        this.serviceName = serviceName;
        this.object = object;
    }

    @Override
    public void run() {
        DistributedObjectEvent event = new DistributedObjectEvent(type, serviceName, object);
        for (DistributedObjectListener listener : listeners) {
            switch (type) {
                case CREATED:
                    listener.distributedObjectCreated(event);
                    break;
                case DESTROYED:
                    listener.distributedObjectDestroyed(event);
                    break;
                default:
                    throw new IllegalStateException("Unrecognized EventType:" + type);
            }
        }
    }

    @Override
    public int getKey() {
        return object.getName().hashCode();
    }
}
