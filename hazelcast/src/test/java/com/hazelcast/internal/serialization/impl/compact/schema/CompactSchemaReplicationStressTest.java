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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationStressTest extends HazelcastTestSupport {

    private static final Random RANDOM = new Random();
    private static final int CLUSTER_SIZE = 6;
    private static final int DRIVER_COUNT = 30;
    private static final int SCHEMA_COUNT = 42000;
    private static final String MAP_NAME = "map";
    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameterized.Parameter
    public DriverType driverType;

    @Parameterized.Parameters(name = "driverType:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {DriverType.CLIENT},
                {DriverType.MEMBER},
        });
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Before
    public void setup() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            factory.newHazelcastInstance(getMemberConfig());
        }
    }

    @Test
    public void testSchemaReplication() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        DriverThread[] driverThreads = new DriverThread[DRIVER_COUNT];
        for (int i = 0; i < DRIVER_COUNT; i++) {
            HazelcastInstance driver = getDriver();
            DriverThread thread = new DriverThread(driver, latch);
            thread.start();
            driverThreads[i] = thread;
        }

        latch.countDown();

        for (int i = 0; i < DRIVER_COUNT; i++) {
            driverThreads[i].join();
        }

        for (int i = 0; i < DRIVER_COUNT; i++) {
            driverThreads[i].assertNoExceptionThrown();
        }
    }

    private HazelcastInstance getDriver() {
        switch (driverType) {
            case CLIENT:
                return factory.newHazelcastClient(getClientConfig());
            case MEMBER:
                return getRandomMember();
            default:
                throw new IllegalStateException("Unknown driver type");
        }
    }

    private HazelcastInstance getRandomMember() {
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        int index = RANDOM.nextInt(instances.size());
        return new ArrayList<>(instances).get(index);
    }

    private ClientConfig getClientConfig() {
        ClientConfig config = new ClientConfig();
        config.getSerializationConfig()
                .getCompactSerializationConfig()
                .setEnabled(true);

        return config;
    }

    private Config getMemberConfig() {
        Config config = new Config();
        config.getSerializationConfig()
                .getCompactSerializationConfig()
                .setEnabled(true);
        return config;
    }

    private enum DriverType {
        CLIENT,
        MEMBER,
    }

    private static class DriverThread extends Thread {

        private final CountDownLatch latch;
        private final IMap<Integer, GenericRecord> map;
        private volatile Throwable t;

        private DriverThread(HazelcastInstance driver, CountDownLatch latch) {
            this.latch = latch;
            map = driver.getMap(MAP_NAME);
        }

        @Override
        public void run() {
            try {
                latch.await();
                replicateSchemas();
            } catch (Throwable t) {
                this.t = t;
            }
        }

        private void replicateSchemas() {
            for (int i = 0; i < SCHEMA_COUNT ; i++) {
                GenericRecord record = GenericRecordBuilder.compact(Integer.toString(i))
                        .build();

                map.put(i, record);
            }
        }

        public void assertNoExceptionThrown() {
            assertNull(t);
        }
    }
}
