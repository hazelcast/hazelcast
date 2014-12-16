package com.hazelcast.map.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.DataSerializable;

/**
 * General contract for map event data.
 */
public interface EventData extends DataSerializable {

    String getSource();

    String getMapName();

    Address getCaller();

    int getEventType();

}
