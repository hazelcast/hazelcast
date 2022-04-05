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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class EntryProcessorOffloadableBouncingNodesTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "EntryProcessorOffloadableTest";
    public static final int COUNT_ENTRIES = 1000;
    private static final int CONCURRENCY = RuntimeAvailableProcessors.get();

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getBouncingTestConfig())
            .driverType(BounceTestConfiguration.DriverType.MEMBER).build();

    private void populateMap(IMap<Integer, SimpleValue> map) {
        for (int i = 0; i < COUNT_ENTRIES; i++) {
            map.put(i, new SimpleValue(i));
        }
    }

    public Config getBouncingTestConfig() {
        Config config = getConfig();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setAsyncBackupCount(1);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);
        return config;
    }

    @Test
    public void testOffloadableEntryProcessor() {
        populateMap(getMap());
        testForKey(0);
    }

    private void testForKey(int key) {
        EntryProcessorRunnable[] tasks = new EntryProcessorRunnable[CONCURRENCY * 2];

        // off-loadable writers
        for (int i = 0; i < CONCURRENCY; i++) {
            tasks[i] = new EntryProcessorRunnable(bounceMemberRule.getNextTestDriver(), new EntryIncOffloadable(), key);
        }

        // off-loadable readers
        for (int i = CONCURRENCY; i < CONCURRENCY * 2; i++) {
            tasks[i] = new EntryProcessorRunnable(bounceMemberRule.getNextTestDriver(), new EntryReadOnlyOffloadable(), key);
        }

        bounceMemberRule.testRepeatedly(tasks, MINUTES.toSeconds(3));
    }

    public static class EntryProcessorRunnable implements Runnable {
        private final IMap map;
        private final EntryProcessor ep;
        private final int key;

        public EntryProcessorRunnable(HazelcastInstance hz, EntryProcessor ep, int key) {
            this.map = hz.getMap(MAP_NAME);
            this.ep = ep;
            this.key = key;
        }

        @Override
        public void run() {
            map.executeOnKey(key, ep);
        }
    }

    private static class EntryIncOffloadable implements EntryProcessor<Integer, SimpleValue, Integer>, Offloadable {

        @Override
        public Integer process(final Map.Entry<Integer, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            int result = value.i;
            value.i++;
            entry.setValue(value);
            sleepAtLeastMillis(10);
            return result;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    private static class EntryReadOnlyOffloadable
            implements EntryProcessor<Integer, SimpleValue, Object>, Offloadable, ReadOnly {

        @Override
        public Object process(final Map.Entry<Integer, SimpleValue> entry) {
            return null;
        }

        @Override
        public EntryProcessor<Integer, SimpleValue, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    private IMap<Integer, SimpleValue> getMap() {
        return bounceMemberRule.getSteadyMember().getMap(MAP_NAME);
    }

    private static class SimpleValue implements Serializable {
        public int i;

        SimpleValue() {
        }

        SimpleValue(final int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimpleValue that = (SimpleValue) o;

            if (i != that.i) {
                return false;
            }

            return true;
        }

        @Override
        public String toString() {
            return "value: " + i;
        }
    }

}
