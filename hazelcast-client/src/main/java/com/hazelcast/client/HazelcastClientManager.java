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
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.internal.util.Trace;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.DefaultClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.Trace.removeThreadLocalTrace;
import static com.hazelcast.internal.util.Trace.setThreadLocalTrace;
import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Central manager for all Hazelcast clients of the JVM.
 * <p>
 * All creation functionality will be stored here and a particular instance of a client will delegate here.
 */
public final class HazelcastClientManager {

    /**
     * Global instance of {@link HazelcastClientManager}
     */
    public static final HazelcastClientManager INSTANCE = new HazelcastClientManager();

    static {
        OutOfMemoryErrorDispatcher.setClientHandler(new ClientOutOfMemoryHandler());
    }

    private final ConcurrentMap<String, HazelcastClientProxy> clients
            = new ConcurrentHashMap<String, HazelcastClientProxy>(5);

    private HazelcastClientManager() {
    }

    public static HazelcastInstance newHazelcastClient(HazelcastClientFactory hazelcastClientFactory) {
        return newHazelcastClient(new XmlClientConfigBuilder().build(), hazelcastClientFactory);
    }

    @SuppressWarnings("unchecked")
    public static HazelcastInstance newHazelcastClient(ClientConfig config, final HazelcastClientFactory hazelcastClientFactory) {
        Trace trace = setThreadLocalTrace(new Trace("HazelcastClient start"));

        try {
            if (config == null) {
                trace.startSubtrace("build xml config");
                try {
                    config = new XmlClientConfigBuilder().build();
                } finally {
                    trace.completeSubtrace();
                }
            }

            final ClassLoader contextClassLoader = trace.subtrace("getting context classloader", new Callable<ClassLoader>() {
                @Override
                public ClassLoader call() {
                    return Thread.currentThread().getContextClassLoader();
                }
            });

            HazelcastClientProxy proxy;
            try {
                trace.subtrace("setting context classloader", new Runnable() {
                    @Override
                    public void run() {
                        Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());

                    }
                });

                final ClientConnectionManagerFactory clientConnectionManagerFactory = trace.subtrace(
                        "creating clientConnectionManagerFactory", new Callable<ClientConnectionManagerFactory>() {
                            @Override
                            public ClientConnectionManagerFactory call() {
                                return new DefaultClientConnectionManagerFactory();
                            }
                        });

                // bleh
                final AtomicReference<ClientConfig> ref = new AtomicReference<ClientConfig>(config);
                final HazelcastClientInstanceImpl client = trace.subtrace("creating HazelcastClientInstanceImpl", new Callable<HazelcastClientInstanceImpl>() {
                    @Override
                    public HazelcastClientInstanceImpl call() throws Exception {
                        return hazelcastClientFactory.createHazelcastInstanceClient(ref.get(),
                                clientConnectionManagerFactory);

                    }
                });

                trace.subtrace("starting HazelcastClientInstanceImpl", new Runnable() {
                    @Override
                    public void run() {
                        client.start();
                    }
                });

                OutOfMemoryErrorDispatcher.registerClient(client);
                proxy = trace.subtrace("create proxy", new Callable<HazelcastClientProxy>() {
                    @Override
                    public HazelcastClientProxy call() {
                        return hazelcastClientFactory.createProxy(client);
                    }
                });

                if (INSTANCE.clients.putIfAbsent(client.getName(), proxy) != null) {
                    throw new DuplicateInstanceNameException("HazelcastClientInstance with name '" + client.getName()
                            + "' already exists!");
                }
            } finally {
                trace.subtrace("restoring context classloader", new Runnable() {
                    @Override
                    public void run() {
                        Thread.currentThread().setContextClassLoader(contextClassLoader);
                    }
                });

            }
            return proxy;
        } finally {
            trace.complete();
            removeThreadLocalTrace();
            String message = trace.toString();
            if (message != null)
                Logger.getLogger(HazelcastClientManager.class).info(message);
        }
    }

    public static HazelcastInstance getHazelcastClientByName(String instanceName) {
        return INSTANCE.clients.get(instanceName);
    }

    public static Collection<HazelcastInstance> getAllHazelcastClients() {
        Collection<HazelcastClientProxy> values = INSTANCE.clients.values();
        return Collections.unmodifiableCollection(new HashSet<HazelcastInstance>(values));
    }

    public static void shutdownAll() {
        for (HazelcastClientProxy proxy : INSTANCE.clients.values()) {
            HazelcastClientInstanceImpl client = proxy.client;
            if (client == null) {
                continue;
            }
            proxy.client = null;
            try {
                client.shutdown();
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        }
        OutOfMemoryErrorDispatcher.clearClients();
        INSTANCE.clients.clear();
    }

    public static void shutdown(HazelcastInstance instance) {
        if (instance instanceof HazelcastClientProxy) {
            final HazelcastClientProxy proxy = (HazelcastClientProxy) instance;
            HazelcastClientInstanceImpl client = proxy.client;
            if (client == null) {
                return;
            }
            proxy.client = null;
            INSTANCE.clients.remove(client.getName());

            try {
                client.shutdown();
            } catch (Throwable ignored) {
                ignore(ignored);
            } finally {
                OutOfMemoryErrorDispatcher.deregisterClient(client);
            }
        }
    }

    public static void shutdown(String instanceName) {
        HazelcastClientProxy proxy = INSTANCE.clients.remove(instanceName);
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
            ignore(ignored);
        } finally {
            OutOfMemoryErrorDispatcher.deregisterClient(client);
        }
    }

    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        OutOfMemoryErrorDispatcher.setClientHandler(outOfMemoryHandler);
    }
}
