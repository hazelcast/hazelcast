package com.hazelcast.client.spi;

import com.hazelcast.client.impl.protocol.ClientMessage;

/**
 * Client service to add/remove remote listeners.
 */
public interface ClientListenerService {

    String startListening(ClientMessage clientMessage, Object key, EventHandler handler);

    boolean stopListening(ClientMessage clientMessage, String registrationId);

    void registerListener(String uuid, Integer callId);

    String deRegisterListener(String uuid);
}
