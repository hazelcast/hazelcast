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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExpirationManagerOverlappingRunsStressTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "mapName";
    private static final int ENTRY_COUNT = 100000;
    private MapServiceContext mapServiceContext;
    private IMap map;

    @Before
    public void setUp() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "1999");
        HazelcastInstance node = createHazelcastInstance(config);
        mapServiceContext
                = ((MapService) getNodeEngineImpl(node).getService(MapService.SERVICE_NAME)).getMapServiceContext();
        map = node.getMap(MAP_NAME);
    }

    @Test
    public void testOverlappingExpirationTaskShouldNotRun() {

        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i, 1, TimeUnit.DAYS);
        }

        ExpirationManager expirationManager = new ExpirationManager(mapServiceContext);
        AtomicBoolean running = new AtomicBoolean(true);
        new Thread(new LastUpdateTimeRunner(mapServiceContext, running)).start();
        expirationManager.startNonProductionTestOnly(10, 1, TimeUnit.MILLISECONDS);

        sleepSeconds(20);
        running.set(false);
    }

    private class LastUpdateTimeRunner implements Runnable {

        private final MapServiceContext mapServiceContext;
        private final Random random = new Random();
        private final AtomicBoolean running;

        private LastUpdateTimeRunner(MapServiceContext mapServiceContext, AtomicBoolean running) {
            this.mapServiceContext = mapServiceContext;
            this.running = running;
        }

        @Override
        public void run() {
            while (running.get()) {
                for (int i = 0; i < mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount(); i++) {
                    PartitionContainer container = mapServiceContext.getPartitionContainer(i);
                    container.setLastCleanupTime(random.nextLong());
                }
            }
        }
    }
}
