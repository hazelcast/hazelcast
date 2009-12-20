/**
 * 
 */
package com.hazelcast.impl.base;

import com.hazelcast.nio.Packet;

public interface PacketProcessor {
    void process(Packet packet);
}