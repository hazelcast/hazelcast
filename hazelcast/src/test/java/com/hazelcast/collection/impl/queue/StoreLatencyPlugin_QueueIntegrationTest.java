/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.TestStringUtils.fileAsText;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StoreLatencyPlugin_QueueIntegrationTest extends HazelcastTestSupport {

    private static final String QUEUE_NAME = "someQueue";

    private HazelcastInstance hz;
    private IQueue<Integer> queue;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.storeLatency.period.seconds", "1");

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
            queue.put(i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
                String content = fileAsText(file);
                assertContains(content, QUEUE_NAME);
            }
        });
    }

    private static QueueConfig addQueueConfig(Config config) {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(new QueueStore() {
                    private final Random random = new Random();

                    @Override
                    public void store(Long key, Object value) {
                        randomSleep();
                    }

                    @Override
                    public void delete(Long key) {
                        randomSleep();
                    }

                    @Override
                    public void storeAll(Map map) {
                        randomSleep();
                    }

                    @Override
                    public void deleteAll(Collection keys) {
                        randomSleep();
                    }

                    @Override
                    public Map loadAll(Collection keys) {
                        randomSleep();
                        return new HashMap();
                    }

                    @Override
                    public Set<Long> loadAllKeys() {
                        return new HashSet<Long>();
                    }

                    @Override
                    public Object load(Long key) {
                        randomSleep();
                        return key;
                    }

                    private void randomSleep() {
                        long delay = random.nextInt(100);
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });

        return config.getQueueConfig(QUEUE_NAME)
                .setQueueStoreConfig(queueStoreConfig);
    }
}
