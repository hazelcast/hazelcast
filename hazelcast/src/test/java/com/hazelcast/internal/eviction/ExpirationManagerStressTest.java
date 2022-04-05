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

package com.hazelcast.internal.eviction;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ExpirationManagerStressTest extends HazelcastTestSupport {

    @Test
    public void ensure_expiration_task_started_after_many_concurrent_start_stops() {
        final ExpirationManager expirationManager = getExpirationManager(createHazelcastInstance());

        final AtomicBoolean stop = new AtomicBoolean(false);
        LinkedList<Thread> threads = new LinkedList<Thread>();

        for (int j = 0; j < 2; j++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        ClusterState[] states = ClusterState.values();
                        for (final ClusterState state : states) {
                            expirationManager.onClusterStateChange(state);
                        }
                        expirationManager.onClusterStateChange(ClusterState.ACTIVE);
                    }
                }
            };

            threads.add(thread);
        }

        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        LifecycleEvent.LifecycleState[] lifecycleStates = LifecycleEvent.LifecycleState.values();
                        for (final LifecycleEvent.LifecycleState lifecycleState : lifecycleStates) {
                            expirationManager.stateChanged(new LifecycleEvent(lifecycleState));
                        }
                        expirationManager.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.MERGED));
                    }
                }
            };

            threads.add(thread);
        }

        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        expirationManager.scheduleExpirationTask();
                        expirationManager.unscheduleExpirationTask();
                        expirationManager.scheduleExpirationTask();
                    }
                }
            };

            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(5);
        stop.set(true);

        assertJoinable(threads.toArray(new Thread[0]));

        assertTrue(expirationManager.isScheduled());
    }

    public static ExpirationManager getExpirationManager(HazelcastInstance node) {
        MapService mapService = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getExpirationManager();
    }
}
