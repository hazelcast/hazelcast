package com.hazelcast.client.impl.protocol.map;

/**
 * Map message types
 */
public enum MapMessageType {

    /**
     * Map put
     */
    MAP_PUT(9),
    /**
     * Map get
     */
    MAP_GET(10);

    private final int id;

    MapMessageType(int messageType) {
        this.id = messageType;
    }

    public int id() {
        return id;
    }

}
