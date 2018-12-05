/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;

import java.util.Collection;

/**
 * The HazelcastClient is comparable to the {@link com.hazelcast.core.Hazelcast} class and provides the ability
 * the create and manage Hazelcast clients. Hazelcast clients are {@link HazelcastInstance} implementations, so
 * in most cases most of the code is unaware of talking to a cluster member or a client.
 * <p/>
 * <h1>Smart vs unisocket clients</h1>
 * Hazelcast Client enables you to do all Hazelcast operations without being a member of the cluster. Clients can be:
 * <ol>
 * <li>smart: this means that they immediately can send an operation like map.get(key) to the member that owns that
 * specific key.
 * </li>
 * <li>
 * unisocket: it will connect to a random member in the cluster and send requests to this member. This member then needs
 * to send the request to the correct member.
 * </li>
 * </ol>
 * For more information see {@link com.hazelcast.client.config.ClientNetworkConfig#setSmartRouting(boolean)}.
 * <p/>
 * <h1>High availability</h1>
 * When the connected cluster member dies, client will automatically switch to another live member.
 */
public final class HazelcastClient {

    private static final HazelcastClientFactory HAZELCAST_CLIENT_FACTORY = new HazelcastClientFactory() {
        @Override
        public HazelcastClientInstanceImpl createHazelcastInstanceClient(ClientConfig config,
                                                                         ClientConnectionManagerFactory factory) {
            return new HazelcastClientInstanceImpl(config, factory, null);
        }

        @Override
        public HazelcastClientProxy createProxy(HazelcastClientInstanceImpl client) {
            return new HazelcastClientProxy(client);
        }
    };

    private HazelcastClient() {
    }

    public static HazelcastInstance newHazelcastClient() {
        return HazelcastClientManager.newHazelcastClient(HAZELCAST_CLIENT_FACTORY);
    }

    public static HazelcastInstance newHazelcastClient(ClientConfig config) {
        return HazelcastClientManager.newHazelcastClient(config, HAZELCAST_CLIENT_FACTORY);
    }

    /**
     * Returns an existing HazelcastClient with instanceName.
     *
     * @param instanceName Name of the HazelcastInstance (client) which can be retrieved by {@link HazelcastInstance#getName()}
     * @return HazelcastInstance
     */
    public static HazelcastInstance getHazelcastClientByName(String instanceName) {
        return HazelcastClientManager.getHazelcastClientByName(instanceName);
    }

    /**
     * Gets an immutable collection of all client HazelcastInstances created in this JVM.
     * <p/>
     * In managed environments such as Java EE or OSGi Hazelcast can be loaded by multiple classloaders. Typically you will get
     * at least one classloader per every application deployed. In these cases only the client HazelcastInstances created
     * by the same application will be seen, and instances created by different applications are invisible.
     * <p/>
     * The returned collection is a snapshot of the client HazelcastInstances. So changes to the client HazelcastInstances
     * will not be visible in this collection.
     *
     * @return the collection of client HazelcastInstances
     */
    public static Collection<HazelcastInstance> getAllHazelcastClients() {
        return HazelcastClientManager.getAllHazelcastClients();
    }

    /**
     * Shuts down all the client HazelcastInstance created in this JVM.
     * <p/>
     * To be more precise it shuts down the HazelcastInstances loaded using the same classloader this HazelcastClient has been
     * loaded with.
     * <p/>
     * This method is mostly used for testing purposes.
     *
     * @see #getAllHazelcastClients()
     */
    public static void shutdownAll() {
        HazelcastClientManager.shutdownAll();
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     *
     * @param instance the hazelcast client instance
     */
    public static void shutdown(HazelcastInstance instance) {
        HazelcastClientManager.shutdown(instance);
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     *
     * @param instanceName the hazelcast client instance name
     */
    public static void shutdown(String instanceName) {
        HazelcastClientManager.shutdown(instanceName);
    }

    /**
     * Sets <tt>OutOfMemoryHandler</tt> to be used when an <tt>OutOfMemoryError</tt>
     * is caught by Hazelcast Client threads.
     * <p/>
     * <p>
     * <b>Warning: </b> <tt>OutOfMemoryHandler</tt> may not be called although JVM throws
     * <tt>OutOfMemoryError</tt>.
     * Because error may be thrown from an external (user thread) thread
     * and Hazelcast may not be informed about <tt>OutOfMemoryError</tt>.
     * </p>
     *
     * @param outOfMemoryHandler set when an <tt>OutOfMemoryError</tt> is caught by HazelcastClient threads
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        HazelcastClientManager.setOutOfMemoryHandler(outOfMemoryHandler);
    }
}
