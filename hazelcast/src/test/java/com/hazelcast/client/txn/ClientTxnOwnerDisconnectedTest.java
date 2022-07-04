/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
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
import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
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
        Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Hazelcast.newHazelcastInstance();
        final TransactionContext context = client.newTransactionContext();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                clientDisconnected.countDown();
            }
        });

        Hazelcast.shutdownAll();

        assertOpenEventually(clientDisconnected);

        context.beginTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testNewTransactionContextShouldFail_onDisconnectedState() {
        Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        Hazelcast.newHazelcastInstance();

        final CountDownLatch clientDisconnected = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                clientDisconnected.countDown();
            }
        });

        Hazelcast.shutdownAll();

        assertOpenEventually(clientDisconnected);

        client.newTransactionContext();
    }

    @Test(expected = TransactionException.class)
    public void testXAShouldFail_onDisconnectedState() throws Throwable {
        Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

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

        Hazelcast.shutdownAll();

        assertOpenEventually(clientDisconnected);

        try {
            transaction.enlistResource(xaResource);
        } finally {
            transaction.rollback();
            tm.close();
            cleanAtomikosLogs();
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
