package com.hazelcast.map;

import com.hazelcast.spi.exception.RetryableHazelcastException;

/**
 * @author enesakar 1/22/14
 */
public class MapNotReadyException extends RetryableHazelcastException {

    public MapNotReadyException() {
        super("Map is still being loaded from map store. If initial load takes much time; try calling map.waitInitialLoad() first.");
    }
}
