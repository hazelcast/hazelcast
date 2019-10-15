/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.DefaultClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.EmptyStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.impl.clientside.FailoverClientConfigSupport.resolveClientConfig;
import static com.hazelcast.client.impl.clientside.FailoverClientConfigSupport.resolveClientFailoverConfig;

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

    static {
        OutOfMemoryErrorDispatcher.setClientHandler(new ClientOutOfMemoryHandler());
    }

    static final ConcurrentMap<String, HazelcastClientProxy> CLIENTS
            = new ConcurrentHashMap<String, HazelcastClientProxy>(5);

    private HazelcastClient() {
    }

    /**
     * Creates a new HazelcastInstance (a new client in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast clients on the same JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all clients on this JVM)
     * call {@link #shutdownAll()}.
     *
     * Hazelcast will look into two places for the configuration file:
     * <ol>
     *     <li>
     *         System property: Hazelcast will first check if "hazelcast.client.config" system property is set to a file or a
     *         {@code classpath:...} path. The configuration can either be an XML or a YAML configuration, distinguished by the
     *         suffix ('.xml' or '.yaml') of the provided configuration file's name
     *         Examples: -Dhazelcast.client.config=C:/myhazelcastclient.xml ,
     *         -Dhazelcast.client.config=classpath:the-hazelcast-config.yaml ,
     *         -Dhazelcast.client.config=classpath:com/mydomain/hazelcast.xml
     *     </li>
     *     <li>
     *         "hazelcast-client.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hazelcast will check classpath for hazelcast-client.xml file.
     *     </li>
     *     <li>
     *         "hazelcast-client.yaml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hazelcast will check classpath for hazelcast-client.yaml file.
     *     </li>
     * </ol>
     * If Hazelcast doesn't find any config file, it will start with the default configuration (hazelcast-client-default.xml)
     * located in hazelcast.jar.
     *
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastClientByName(String) (String)
     */
    public static HazelcastInstance newHazelcastClient() {
        return newHazelcastClientInternal(null, resolveClientConfig(null), null);
    }

    /**
     * Creates a new HazelcastInstance (a new client in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast clients on the same JVM.
     * <p/>
     * To shutdown all running HazelcastInstances (all clients on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new HazelcastInstance (member)
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastClientByName(String) (String)
     */
    public static HazelcastInstance newHazelcastClient(ClientConfig config) {
        return newHazelcastClientInternal(null, resolveClientConfig(config), null);
    }

    /**
     * Creates a client with cluster switch capability. Client will try to connect to alternative clusters according to
     * {@link ClientFailoverConfig} when it disconnects from a cluster.
     *
     * The configuration is loaded using using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hazelcast.client.failover.config' is set. If it exist and it begins with
     * 'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference. The configuration can
     * either be an XML or a YAML configuration, distinguished by the suffix ('.xml' or '.yaml') of the provided configuration
     * file's name</li>
     * <li>it checks if a hazelcast-client-failover.xml is available in the working dir</li>
     * <li>it checks if a hazelcast-client-failover.xml is available on the classpath</li>
     * <li>it checks if a hazelcast-client-failover.yaml is available in the working dir</li>
     * <li>it checks if a hazelcast-client-failover.yaml is available on the classpath</li>
     * <li>if none available {@link HazelcastException} is thrown</li>
     * </ol>
     *
     * @return the client instance
     * @throws HazelcastException            if no failover configuration is found
     * @throws InvalidConfigurationException if the loaded failover configuration is not valid
     */
    public static HazelcastInstance newHazelcastFailoverClient() {
        return newHazelcastClientInternal(null, null, resolveClientFailoverConfig());
    }

    /**
     * Creates a client with cluster switch capability. Client will try to connect to alternative clusters according to
     * {@link ClientFailoverConfig} when it disconnects from a cluster.
     *
     * If provided clientFailoverConfig is {@code null}, the configuration is loaded using using the following resolution
     * mechanism:
     * <ol>
     *      <li>first it checks if a system property 'hazelcast.client.failover.config' is set. If it exist and it begins with
     *          'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference. The configuration
     *          can either be an XML or a YAML configuration, distinguished by the suffix ('.xml' or '.yaml') of the provided
     *          configuration file's name</li>
     *      <li>it checks if a hazelcast-client-failover.xml is available in the working dir</li>
     *      <li>it checks if a hazelcast-client-failover.xml is available on the classpath</li>
     *      <li>it checks if a hazelcast-client-failover.yaml is available in the working dir</li>
     *      <li>it checks if a hazelcast-client-failover.yaml is available on the classpath</li>
     *      <li>if none available {@link HazelcastException} is thrown</li>
     * </ol>
     *
     *
     * @param clientFailoverConfig config describing the failover configs and try count
     * @return the client instance
     * @throws HazelcastException            if no failover configuration is found
     * @throws InvalidConfigurationException if the provided or the loaded failover configuration is not valid
     */
    public static HazelcastInstance newHazelcastFailoverClient(ClientFailoverConfig clientFailoverConfig) {
        return newHazelcastClientInternal(null, null, resolveClientFailoverConfig(clientFailoverConfig));
    }

    static HazelcastInstance newHazelcastClientInternal(AddressProvider addressProvider, ClientConfig clientConfig,
                                                        ClientFailoverConfig failoverConfig) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        HazelcastClientProxy proxy;
        try {
            Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            ClientConnectionManagerFactory factory = new DefaultClientConnectionManagerFactory();
            HazelcastClientInstanceImpl client =
                    new HazelcastClientInstanceImpl(clientConfig, failoverConfig, factory, addressProvider);
            client.start();
            OutOfMemoryErrorDispatcher.registerClient(client);
            proxy = new HazelcastClientProxy(client);

            if (CLIENTS.putIfAbsent(client.getName(), proxy) != null) {
                throw new InvalidConfigurationException("HazelcastClientInstance with name '" + client.getName()
                        + "' already exists!");
            }
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
        return proxy;
    }

    /**
     * Returns an existing HazelcastClient with instanceName.
     *
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
        return Collections.unmodifiableCollection(new HashSet<HazelcastInstance>(values));
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
     *
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
     *
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
        OutOfMemoryErrorDispatcher.setClientHandler(outOfMemoryHandler);
    }
}
