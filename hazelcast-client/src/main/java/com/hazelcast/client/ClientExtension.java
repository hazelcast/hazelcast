/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;

/**
 * ClientExtension is a client extension mechanism to be able to plug different implementations of
 * some modules, like; {@link InternalSerializationService}, {@link SocketChannelWrapperFactory} etc.
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
     * Creates a {@link InternalSerializationService} instance to be used by this client.
     *
     * @param version serialization version to be created. Values less than 1 will be ignored and max supported version
     * will be used
     * @return the created {@link InternalSerializationService} instance
     */
    InternalSerializationService createSerializationService(byte version);

    /**
     * Creates a {@link SocketInterceptor} to be used by this client if available,
     * otherwise returns <code>null</code>
     *
     * @return the created {@link SocketInterceptor} instance if available,
     * +          otherwise <code>null</code>
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

    /**
     * Creates a {@code ClientProxyFactory} for the supplied service class.
     *
     * @param service service for the proxy to create.
     * @return {@code ClientProxyFactory} for the service.
     * @throws java.lang.IllegalArgumentException if service is not known
     */
    <T> ClientProxyFactory createServiceProxyFactory(Class<T> service);
}
