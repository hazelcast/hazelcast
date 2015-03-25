/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client;

import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;

/**
 * ClientExtension is a client extension mechanism to be able to plug different implementations of
 * some modules, like; {@link SerializationService}, {@link SocketChannelWrapperFactory} etc.
 * some modules, like; <tt>SerializationService</tt>, <tt>SocketChannelWrapperFactory</tt> etc.
 */
public interface ClientExtension {

    /**
     * Called before client is started
     */
    void beforeStart(HazelcastClientInstanceImpl client);

    /**
     * Called after node is started
     */
    void afterStart(HazelcastClientInstanceImpl client);

    /**
     * Creates a {@link SerializationService} instance to be used by this client.
     *
     * @return the created {@link SerializationService} instance
     */
    SerializationService createSerializationService();

    /**
     * Creates a {@link SocketInterceptor} to be used by this client if available,
     * otherwise returns <code>null</code>.
     *
     * @return the created {@link SocketInterceptor} instance if available,
     *         otherwise <code>null</code>
     */
    SocketInterceptor createSocketInterceptor();

    /**
     * Creates a {@link SocketChannelWrapperFactory} instance to be used by this client.
     *
     * @return the created {@link SocketChannelWrapperFactory} instance
     */
    SocketChannelWrapperFactory createSocketChannelWrapperFactory();

    /**
     * Creates a {@link NearCacheManager} instance to be used by this client.
     *
     * @return the created {@link NearCacheManager} instance
     */
    NearCacheManager createNearCacheManager();

    SocketChannelWrapperFactory getSocketChannelWrapperFactory();

    /**
     * Returns a proxy for the corresponding service.
     *
     * @param service service for the proxy to create.
     * @return proxy for the service.
     * @throws java.lang.IllegalArgumentException if type is not known
     */
    <T> Class<? extends ClientProxy> getServiceProxy(Class<T> service);
}
