package com.hazelcast.spi.impl;

import com.hazelcast.spi.NodeEngine;

/**
 * The InternalNodeEngine extends the NodeEngine and exposes all kinds of other internal services that
 * are not exposed to the regular SPI user.
 */
public interface InternalNodeEngine extends NodeEngine{

    ServiceManager getServiceManager();
}
