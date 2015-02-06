package com.hazelcast.spi.impl;

import com.hazelcast.spi.NodeEngine;

/**
 * The InternalNodeEngine extends the {@link NodeEngine} and exposes all kinds of other internal services that
 * are not exposed to the regular SPI user.
 */
public interface InternalNodeEngine extends NodeEngine {

    /**
     * Gets the ServiceManager
     *
     * @return the ServiceManager.
     */
    ServiceManager getServiceManager();
}
