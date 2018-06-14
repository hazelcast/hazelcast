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

package com.hazelcast.client.txn;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientFactory;
import com.hazelcast.client.HazelcastClientManager;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.transaction.Transaction;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientTxnOwnerDisconnectedTest extends ClientTestSupport {

    @After
    public void after() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(expected = TransactionException.class)
    public void testTransactionBeginShouldFail_onDisconnectedState() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        final AtomicBoolean waitFlag = new AtomicBoolean();
        final CountDownLatch testFinished = new CountDownLatch(1);
        final AddressProvider addressProvider = new AddressProvider() {
            @Override
            public Collection<Address> loadAddresses() {
                if (waitFlag.get()) {
                    try {
                        testFinished.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return AddressHelper.getSocketAddresses("127.0.0.1");
            }
        };
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        final HazelcastInstance client = HazelcastClientManager.newHazelcastClient(clientConfig, new HazelcastClientFactory() {
            @Override
            public HazelcastClientInstanceImpl createHazelcastInstanceClient(ClientConfig config, ClientConnectionManagerFactory factory) {
                return new HazelcastClientInstanceImpl(config, factory, addressProvider);
            }

            @Override
            public HazelcastClientProxy createProxy(HazelcastClientInstanceImpl client) {
                return new HazelcastClientProxy(client);
            }
        });

        Hazelcast.newHazelcastInstance();
        final TransactionContext context = client.newTransactionContext();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                clientDisconnected.countDown();
            }
        });

        //we are closing owner connection and making sure owner connection is not established ever again
        waitFlag.set(true);
        instance.shutdown();

        assertOpenEventually(clientDisconnected);

        try {
            context.beginTransaction();
        } finally {
            testFinished.countDown();
        }

    }

    @Test(expected = TransactionException.class)
    public void testNewTransactionContextShouldFail_onDisconnectedState() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        final AtomicBoolean waitFlag = new AtomicBoolean();
        final CountDownLatch testFinished = new CountDownLatch(1);
        final AddressProvider addressProvider = new AddressProvider() {
            @Override
            public Collection<Address> loadAddresses() {
                if (waitFlag.get()) {
                    try {
                        testFinished.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return AddressHelper.getSocketAddresses("127.0.0.1");
            }
        };
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        final HazelcastInstance client = HazelcastClientManager.newHazelcastClient(clientConfig, new HazelcastClientFactory() {
            @Override
            public HazelcastClientInstanceImpl createHazelcastInstanceClient(ClientConfig config, ClientConnectionManagerFactory factory) {
                return new HazelcastClientInstanceImpl(config, factory, addressProvider);
            }

            @Override
            public HazelcastClientProxy createProxy(HazelcastClientInstanceImpl client) {
                return new HazelcastClientProxy(client);
            }
        });


        Hazelcast.newHazelcastInstance();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                clientDisconnected.countDown();
            }
        });

        //we are closing owner connection and making sure owner connection is not established ever again
        waitFlag.set(true);
        instance.shutdown();

        assertOpenEventually(clientDisconnected);

        try {
            client.newTransactionContext();
        } finally {
            testFinished.countDown();
        }

    }

    @Test(expected = TransactionException.class)
    public void testXAShouldFail_onDisconnectedState() throws Throwable {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        final AtomicBoolean waitFlag = new AtomicBoolean();
        final CountDownLatch testFinished = new CountDownLatch(1);
        final AddressProvider addressProvider = new AddressProvider() {
            @Override
            public Collection<Address> loadAddresses() {
                if (waitFlag.get()) {
                    try {
                        testFinished.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return AddressHelper.getSocketAddresses("127.0.0.1");
            }
        };
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        clientConfig.setProperty(ClientProperty.ALLOW_INVOCATIONS_WHEN_DISCONNECTED.getName(), "true");
        final HazelcastInstance client = HazelcastClientManager.newHazelcastClient(clientConfig, new HazelcastClientFactory() {
            @Override
            public HazelcastClientInstanceImpl createHazelcastInstanceClient(ClientConfig config, ClientConnectionManagerFactory factory) {
                return new HazelcastClientInstanceImpl(config, factory, addressProvider);
            }

            @Override
            public HazelcastClientProxy createProxy(HazelcastClientInstanceImpl client) {
                return new HazelcastClientProxy(client);
            }
        });

        Hazelcast.newHazelcastInstance();


        HazelcastXAResource xaResource = client.getXAResource();
        UserTransactionManager tm = new UserTransactionManager();
        cleanAtomikosLogs();
        tm.setTransactionTimeout(3);
        tm.begin();
        Transaction transaction = tm.getTransaction();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                clientDisconnected.countDown();
            }
        });

        //we are closing owner connection and making sure owner connection is not established ever again
        waitFlag.set(true);
        instance.shutdown();

        assertOpenEventually(clientDisconnected);

        try {
            transaction.enlistResource(xaResource);
        } finally {
            transaction.rollback();
            tm.close();
            cleanAtomikosLogs();
            testFinished.countDown();
        }


    }

    public void cleanAtomikosLogs() {
        try {
            File currentDir = new File(".");
            final File[] tmLogs = currentDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".epoch") || name.startsWith("tmlog")) {
                        return true;
                    }
                    return false;
                }
            });
            for (File tmLog : tmLogs) {
                tmLog.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
