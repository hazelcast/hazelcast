package com.hazelcast.spi.impl.proxyservice;

import com.hazelcast.spi.ProxyService;

/**
 * The API for the internal {@link com.hazelcast.spi.ProxyService}.
 *
 * The ProxyService is responsible for managing proxies and it part of the SPI. The InternalProxyService extends this
 * interface and additional methods we don't want to expose in the SPI, we can add here.
 */
public interface InternalProxyService extends ProxyService {
}
