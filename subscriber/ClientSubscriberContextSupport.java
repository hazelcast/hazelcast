package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapDestroyCacheCodec;
import com.hazelcast.client.impl.protocol.codec.EnterpriseMapSetReadCursorCodec;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContextSupport;

/**
 * {@code SubscriberContextSupport} implementation for client side.
 *
 * @see SubscriberContextSupport
 */
public class ClientSubscriberContextSupport implements SubscriberContextSupport {

    public ClientSubscriberContextSupport() {
    }

    @Override
    public Object createRecoveryOperation(String mapName, String cacheName, long sequence, int partitionId) {
        return EnterpriseMapSetReadCursorCodec.encodeRequest(mapName, cacheName, sequence);
    }

    @Override
    public Boolean resolveResponseForRecoveryOperation(Object object) {
        return EnterpriseMapSetReadCursorCodec.decodeResponse((ClientMessage) object).response;
    }

    @Override
    public Object createDestroyQueryCacheOperation(String mapName, String cacheName) {
        return EnterpriseMapDestroyCacheCodec.encodeRequest(mapName, cacheName);
    }
}
