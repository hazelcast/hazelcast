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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackupEntryProcessorInstanceAwareTest extends HazelcastTestSupport {
    public static final String MAP_NAME = "EntryProcessorTest";

    @Override
    public Config getConfig() {
        final MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setReadBackupData(true)
                .setInMemoryFormat(BINARY);
        return super.getConfig().addMapConfig(mapConfig);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        final Config cfg = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance i1 = factory.newHazelcastInstance(cfg);
        final HazelcastInstance i2 = factory.newHazelcastInstance(cfg);
        final IMap<String, Integer> m1 = i1.getMap(MAP_NAME);
        final IMap<String, Integer> m2 = i2.getMap(MAP_NAME);

        m1.put("a", 1);
        m1.put("b", 2);
        m1.executeOnEntries(new PartitionAwareTestEntryProcessor());

        for (final String key : m1.keySet()) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    final Integer k = m1.get(key);
                    final Integer v = m2.get(key);
                    assertEquals(k, v);
                }
            });
        }
        i1.shutdown();
    }

    private static class PartitionAwareTestEntryProcessor
            implements EntryProcessor<String, Integer, Object>, HazelcastInstanceAware {

        private transient HazelcastInstance hz;

        @Override
        public Object process(Map.Entry<String, Integer> entry) {
            if (hz != null) {
                entry.setValue(entry.getValue() + 1);
            }
            return null;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }
}
