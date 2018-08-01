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

package com.hazelcast.internal.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapExpirationManagerTest extends AbstractExpirationManagerTest {

    @Test
    public void restarts_running_backgroundClearTask_when_lifecycleState_turns_to_MERGED() {
        Config config = new Config();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicInteger expirationCounter = new AtomicInteger();

        IMap<Integer, Integer> map = node.getMap("test");
        map.addEntryListener(new EntryExpiredListener() {
            @Override
            public void entryExpired(EntryEvent event) {
                expirationCounter.incrementAndGet();
            }
        }, true);

        map.put(1, 1, 3, TimeUnit.SECONDS);

        ((LifecycleServiceImpl) node.getLifecycleService()).fireLifecycleEvent(MERGING);
        ((LifecycleServiceImpl) node.getLifecycleService()).fireLifecycleEvent(MERGED);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int expirationCount = expirationCounter.get();
                assertEquals(format("Expecting 1 expiration but found:%d", expirationCount), 1, expirationCount);
            }
        });
    }

    @Test
    public void clearExpiredRecordsTask_should_not_be_started_if_map_has_no_expirable_records() {
        Config config = new Config();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        final HazelcastInstance node = createHazelcastInstance(config);

        IMap<Integer, Integer> map = node.getMap("test");
        map.put(1, 1);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse("There should be zero ClearExpiredRecordsTask",
                        hasClearExpiredRecordsTaskStarted(node));
            }
        }, 3);
    }

    @Test
    public void clearExpiredRecordsTask_should_not_be_started_if_member_is_lite() {
        Config liteMemberConfig = new Config();
        liteMemberConfig.setLiteMember(true);
        liteMemberConfig.setProperty(taskPeriodSecondsPropName(), "1");

        Config dataMemberConfig = new Config();
        dataMemberConfig.setProperty(taskPeriodSecondsPropName(), "1");

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance liteMember = factory.newHazelcastInstance(liteMemberConfig);
        final HazelcastInstance dataMember = factory.newHazelcastInstance(dataMemberConfig);

        IMap<Integer, Integer> map = liteMember.getMap("test");
        map.put(1, 1, 3, TimeUnit.SECONDS);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertFalse("There should be zero ClearExpiredRecordsTask",
                        hasClearExpiredRecordsTaskStarted(liteMember));
            }
        }, 3);
    }

    @Test
    public void clearExpiredRecordsTask_should_be_started_when_mapConfig_ttl_expiry() {
        String mapName = "test";

        Config config = new Config();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        config.getMapConfig(mapName).setTimeToLiveSeconds(2);
        HazelcastInstance node = createHazelcastInstance(config);

        IMap<Integer, Integer> map = node.getMap(mapName);
        map.put(1, 1);

        assertTrue("There should be one ClearExpiredRecordsTask",
                hasClearExpiredRecordsTaskStarted(node));
    }

    @Test
    public void clearExpiredRecordsTask_should_be_started_when_mapConfig_has_idle_expiry() {
        String mapName = "test";

        Config config = new Config();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        config.getMapConfig(mapName).setMaxIdleSeconds(2);
        HazelcastInstance node = createHazelcastInstance(config);

        IMap<Integer, Integer> map = node.getMap(mapName);
        map.put(1, 1);

        assertTrue("There should be one ClearExpiredRecordsTask",
                hasClearExpiredRecordsTaskStarted(node));
    }

    private boolean hasClearExpiredRecordsTaskStarted(HazelcastInstance node) {
        MapService service = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        return service.getMapServiceContext().getExpirationManager().isScheduled();
    }

    @Test
    public void stops_running_backgroundClearTask_when_lifecycleState_SHUTTING_DOWN() {
        backgroundClearTaskStops_whenLifecycleState(SHUTTING_DOWN);
    }

    @Test
    public void stops_running_backgroundClearTask_when_lifecycleState_MERGING() {
        backgroundClearTaskStops_whenLifecycleState(MERGING);
    }

    protected PartitionContainer[] getPartitionContainers(HazelcastInstance instance) {
        return ((MapService) getNodeEngineImpl(instance)
                .getService(SERVICE_NAME))
                .getMapServiceContext()
                .getPartitionContainers();
    }

    protected String cleanupOperationCountPropName() {
        return MapClearExpiredRecordsTask.PROP_CLEANUP_OPERATION_COUNT;
    }

    protected String taskPeriodSecondsPropName() {
        return MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
    }

    protected String cleanupPercentagePropName() {
        return MapClearExpiredRecordsTask.PROP_CLEANUP_PERCENTAGE;
    }

    private void backgroundClearTaskStops_whenLifecycleState(LifecycleEvent.LifecycleState lifecycleState) {
        Config config = new Config();
        config.setProperty(taskPeriodSecondsPropName(), "1");
        HazelcastInstance node = createHazelcastInstance(config);

        final AtomicInteger expirationCounter = new AtomicInteger();

        IMap<Integer, Integer> map = node.getMap("test");
        map.addEntryListener(new EntryExpiredListener() {
            @Override
            public void entryExpired(EntryEvent event) {
                expirationCounter.incrementAndGet();
            }
        }, true);

        map.put(1, 1, 3, TimeUnit.SECONDS);

        ((LifecycleServiceImpl) node.getLifecycleService()).fireLifecycleEvent(lifecycleState);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                int expirationCount = expirationCounter.get();
                assertEquals(format("Expecting no expiration but found:%d", expirationCount), 0, expirationCount);
            }
        }, 5);
    }

    protected ExpirationManager newExpirationManager(HazelcastInstance node) {
        return new ExpirationManager(new MapClearExpiredRecordsTask(getNodeEngineImpl(node), getPartitionContainers(node)), getNodeEngineImpl(node));
    }
}
