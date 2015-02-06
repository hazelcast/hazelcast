package com.hazelcast.spi.impl;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.spi.NodeEngine;

/**
 * The InternalNodeEngine extends the {@link NodeEngine} and exposes all kinds of other internal services that
 * are not exposed to the regular SPI user.
 *
 * The InternalNodeEngine is purely an 'umbrella' to inject dependencies. It should not contain all kinds of convenience
 * methods because then it will become polluted + more difficult to test. So don't add methods like 'toObject' or 'toData';
 * let this be a concern of the appropriate dependency.
 */
public interface InternalNodeEngine extends NodeEngine {

    /**
     * Gets the ServiceManager.
     *
     * @return the ServiceManager.
     */
    ServiceManager getServiceManager();

    /**
     * Returns the HazelcastThreadGroup.
     *
     * @return the HazelcastThreadGroup.
     */
    HazelcastThreadGroup getHazelcastThreadGroup();

    /**
     * Returns the GroupProperties.
     *
     * @return the groupProperties.
     */
    GroupProperties getGroupProperties();

    /**
     * Gets the PacketTransceiver.
     *
     * @return the PacketTransceiver.
     */
    PacketTransceiver getPacketTransceiver();
}
