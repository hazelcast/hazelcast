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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.RingbufferStore;
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
import java.util.Random;

import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.TestStringUtils.fileAsText;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StoreLatencyPluginRingbufferIntegrationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private Ringbuffer<Object> rb;

    @Before
    public void setup() throws Exception {
        Config config = new Config()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.storeLatency.period.seconds", "1");

        RingbufferConfig rbConfig = addRingbufferConfig(config);

        hz = createHazelcastInstance(config);
        rb = hz.getRingbuffer(rbConfig.getName());
    }

    @After
    public void after() {
        File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
        deleteQuietly(file);
    }

    @Test
    public void test() throws Exception {
        for (long k = 0; k <= rb.tailSequence(); k++) {
            rb.readOne(k);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                File file = getNodeEngineImpl(hz).getDiagnostics().currentFile();
                String content = fileAsText(file);
                assertContains(content, "ringworm");
            }
        });
    }

    private static RingbufferConfig addRingbufferConfig(Config config) {
        RingbufferStore store = new RingbufferStore() {
            private final Random random = new Random();

            @Override
            public void store(long sequence, Object data) {
            }

            @Override
            public void storeAll(long firstItemSequence, Object[] items) {
            }

            @Override
            public Object load(long sequence) {
                randomSleep();
                return sequence;
            }

            @Override
            public long getLargestSequence() {
                randomSleep();
                return 100;
            }

            private void randomSleep() {
                long delay = 1 + random.nextInt(100);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        return config.getRingbufferConfig("ringworm")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setStoreImplementation(store));
    }
}
