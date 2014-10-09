/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;

/**
 * ClientExtension is a client extension mechanism to be able to plug different implementations of
 * some modules, like; <tt>SerializationService</tt>, <tt>SocketChannelWrapperFactory</tt> etc.
 *
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
     * Creates a <tt>SerializationService</tt> instance to be used by this client.
     *
     * @return a <tt>SerializationService</tt> instance
     */
    SerializationService createSerializationService();

    /**
     * Returns <tt>SocketInterceptor</tt> for this client if available,
     * otherwise returns null.
     *
     * @return SocketInterceptor
     */
    SocketInterceptor getSocketInterceptor();

    /**
     * Returns <tt>SocketChannelWrapperFactory</tt> instance to be used by this client.
     *
     * @return SocketChannelWrapperFactory
     */
    SocketChannelWrapperFactory getSocketChannelWrapperFactory();
}
