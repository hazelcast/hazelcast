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

package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class AtomicLongStableReadStressTest extends StressTestSupport {

    public static final int CLIENT_THREAD_COUNT = 5;
    public static final int REFERENCE_COUNT = 10 * 1000;

    private HazelcastInstance client;
    private IAtomicLong[] references;
    private StressThread[] stressThreads;

    @Before
    public void setUp() {
        super.setUp();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);
        references = new IAtomicLong[REFERENCE_COUNT];
        for (int k = 0; k < references.length; k++) {
            references[k] = client.getAtomicLong("atomicreference:" + k);
        }

        stressThreads = new StressThread[CLIENT_THREAD_COUNT];
        for (int k = 0; k < stressThreads.length; k++) {
            stressThreads[k] = new StressThread();
            stressThreads[k].start();
        }
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.shutdown();
        }

        super.tearDown();
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/4179
    public void testChangingCluster() {
        test(true);
    }

    @Test(timeout = 600000)
    public void testFixedCluster() {
        test(false);
    }

    public void test(boolean clusterChangeEnabled) {
        setClusterChangeEnabled(clusterChangeEnabled);
        initializeReferences();
        startAndWaitForTestCompletion();
        joinAll(stressThreads);
    }

    private void initializeReferences() {
        System.out.println("==================================================================");
        System.out.println("Initializing references");
        System.out.println("==================================================================");

        for (int k = 0; k < references.length; k++) {
            references[k].set(k);
        }

        System.out.println("==================================================================");
        System.out.println("Completed with initializing references");
        System.out.println("==================================================================");
    }

    public class StressThread extends TestThread {

        @Override
        public void doRun() throws Exception {
            while (!isStopped()) {
                int key = random.nextInt(REFERENCE_COUNT);
                IAtomicLong reference = references[key];
                long value = reference.get();
                assertEquals(format("The value for atomic reference: %s was not consistent", reference), key, value);
            }
        }
    }
}
