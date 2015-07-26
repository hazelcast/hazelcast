package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.map.impl.client.DestroyQueryCacheRequest;
import com.hazelcast.map.impl.client.SetReadCursorRequest;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContextSupport;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * {@code SubscriberContextSupport} implementation for client side.
 *
 * @see SubscriberContextSupport
 */
public class ClientSubscriberContextSupport implements SubscriberContextSupport {

    private final SerializationService serializationService;

    public ClientSubscriberContextSupport(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Object createRecoveryOperation(String mapName, String cacheName, long sequence, int partitionId) {
        return new SetReadCursorRequest(mapName, cacheName, sequence, partitionId);
    }

    @Override
    public Boolean resolveResponseForRecoveryOperation(Object response) {
        return (Boolean) serializationService.toObject(response);
    }

    @Override
    public Object createDestroyQueryCacheOperation(String mapName, String cacheName) {
        return new DestroyQueryCacheRequest(mapName, cacheName);
    }
}
