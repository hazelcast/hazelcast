package com.hazelcast.client.spi;

import com.hazelcast.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.ClientRequest;

/**
 * Client service to add/remove remote listeners.
 */
public interface ClientListenerService {

    String listen(ClientRequest request, Object key, EventHandler handler);

    boolean stopListening(BaseClientRemoveListenerRequest request, String registrationId);

}
