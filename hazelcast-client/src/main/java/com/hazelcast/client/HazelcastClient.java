/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The HazelcastClient is comparable to the {@link com.hazelcast.core.Hazelcast} class and provides the ability
 * the create and manage Hazelcast clients. Hazelcast clients are {@link HazelcastInstance} implementations, so
 * in most cases most of the code is unaware of talking to a cluster member or a client.
 * <p/>
 * <h1>Smart vs dumb clients</h1>
 * Hazelcast Client enables you to do all Hazelcast operations without being a member of the cluster. Clients can be:
 * <ol>
 * <li>smart: this means that they immediately can send an operation like map.get(key) to the member that owns that
 * specific key.
 * </li>
 * <li>
 * dumb: it will connect to a random member in the cluster and send requests to this member. This member then needs
 * to send the request to the correct member.
 * </li>
 * </ol>
 * For more information see {@link com.hazelcast.client.config.ClientNetworkConfig#setSmartRouting(boolean)}.
 * <p/>
 * <h1>High availability</h1>
 * When the connected cluster member dies, client will automatically switch to another live member.
 */
public final class HazelcastClient {

    static {
        OutOfMemoryErrorDispatcher.setClientHandler(new ClientOutOfMemoryHandler());
    }

    private static final ConcurrentMap<String, HazelcastClientProxy> CLIENTS
            = new ConcurrentHashMap<String, HazelcastClientProxy>(5);

    // we don't want instances.
    private HazelcastClient() {
    }

    public static HazelcastInstance newHazelcastClient() {
        return newHazelcastClient(new XmlClientConfigBuilder().build());
    }

    public static HazelcastInstance newHazelcastClient(ClientConfig config) {
        if (config == null) {
            config = new XmlClientConfigBuilder().build();
        }

        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        HazelcastClientProxy proxy;
        try {
            Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            final HazelcastClientInstanceImpl client = new HazelcastClientInstanceImpl(config);
            client.start();
            OutOfMemoryErrorDispatcher.registerClient(client);
            proxy = new HazelcastClientProxy(client);
            CLIENTS.put(client.getName(), proxy);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
        return proxy;
    }

    /**
     * Returns an existing HazelcastClient with instanceName.
     * <p/>
     * @param instanceName Name of the HazelcastInstance (client) which can be retrieved by {@link HazelcastInstance#getName()}
     * @return HazelcastInstance
     */
    public static HazelcastInstance getHazelcastClientByName(String instanceName) {
        return CLIENTS.get(instanceName);
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
        Collection<HazelcastClientProxy> values = CLIENTS.values();
        return Collections.<HazelcastInstance>unmodifiableCollection(new HashSet<HazelcastInstance>(values));
    }

    /**
     * Shuts down all the client HazelcastInstance created in this JVM.
     * <p/>
     * To be more precise it shuts down the HazelcastInstances loaded using the same classloader this HazelcastClient has been
     * loaded with.
     *
     * This method is mostly used for testing purposes.
     *
     * @see #getAllHazelcastClients()
     */
    public static void shutdownAll() {
        for (HazelcastClientProxy proxy : CLIENTS.values()) {
            HazelcastClientInstanceImpl client = proxy.client;
            if (client == null) {
                continue;
            }
            proxy.client = null;
            try {
                client.shutdown();
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            }
        }
        OutOfMemoryErrorDispatcher.clearClients();
        CLIENTS.clear();
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     * @param instance the hazelcast client instance
     */
    public static void shutdown(HazelcastInstance instance) {
        if (instance instanceof HazelcastClientProxy) {
            final HazelcastClientProxy proxy = (HazelcastClientProxy) instance;
            HazelcastClientInstanceImpl client = proxy.client;
            if (client == null) {
                return;
            }
            proxy.client = null;
            CLIENTS.remove(client.getName());

            try {
                client.shutdown();
            } catch (Throwable ignored) {
                EmptyStatement.ignore(ignored);
            } finally {
                OutOfMemoryErrorDispatcher.deregisterClient(client);
            }
        }
    }

    /**
     * Shutdown the provided client and remove it from the managed list
     * @param instanceName the hazelcast client instance name
     */
    public static void shutdown(String instanceName) {
        HazelcastClientProxy proxy = CLIENTS.remove(instanceName);
        if (proxy == null) {
            return;
        }
        HazelcastClientInstanceImpl client = proxy.client;
        if (client == null) {
            return;
        }
        proxy.client = null;
        try {
            client.shutdown();
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        } finally {
            OutOfMemoryErrorDispatcher.deregisterClient(client);
        }
    }

}
