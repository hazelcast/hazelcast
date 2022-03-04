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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.TestStringUtils.fileAsText;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StoreLatencyPlugin_QueueIntegrationTest extends HazelcastTestSupport {

    private static final ILogger LOG = Logger.getLogger(StoreLatencyPlugin_QueueIntegrationTest.class);

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    private static final String QUEUE_NAME = "someQueue";

    private HazelcastInstance hz;
    private IQueue<VersionedObject<Integer>> queue;

    @Before
    public void setup() {
        Config config = smallInstanceConfig()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.storeLatency.period.seconds", "1");

        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);

        QueueConfig queueConfig = addQueueConfig(config);

        hz = createHazelcastInstance(config);
        queue = hz.getQueue(queueConfig.getName());
    }

    @After
    public void after() {
        File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
        deleteQuietly(file);
    }

    @Test
    public void test() throws Exception {
        for (int i = 0; i < 100; i++) {
            queue.put(new VersionedObject<>(i));
        }

        assertTrueEventually(() -> {
            File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
            String content = fileAsText(file);
            assertContains(content, QUEUE_NAME);
        });
    }

    private static QueueConfig addQueueConfig(Config config) {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(new QueueStore<VersionedObject<Integer>>() {
                    private final Random random = new Random();

                    @Override
                    public void store(Long key, VersionedObject<Integer> value) {
                        randomSleep();
                    }

                    @Override
                    public void delete(Long key) {
                        randomSleep();
                    }

                    @Override
                    public void storeAll(Map<Long, VersionedObject<Integer>> map) {
                        randomSleep();
                    }

                    @Override
                    public void deleteAll(Collection<Long> keys) {
                        randomSleep();
                    }

                    @Override
                    public Map<Long, VersionedObject<Integer>> loadAll(Collection<Long> keys) {
                        randomSleep();
                        return new HashMap<>();
                    }

                    @Override
                    public Set<Long> loadAllKeys() {
                        return new HashSet<>();
                    }

                    @Override
                    public VersionedObject<Integer> load(Long key) {
                        randomSleep();
                        return new VersionedObject<>(random.nextInt());
                    }

                    private void randomSleep() {
                        long delay = random.nextInt(100);
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            LOG.info(e);
                        }
                    }
                });

        return config.getQueueConfig(QUEUE_NAME)
                     .setQueueStoreConfig(queueStoreConfig);
    }
}
