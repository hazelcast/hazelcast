/**
 * 
 */
package com.hazelcast.impl.base;

import com.hazelcast.impl.Processable;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;

public interface Call extends Processable {

    long getCallId();

    void handleResponse(Packet packet);

    void onDisconnect(Address dead);

    void setCallId(long id);
}