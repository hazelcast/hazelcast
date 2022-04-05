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

package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGE_FAILED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;

/**
 * Protects against in-progress merging.
 */
class MergeBarrier {

    private final AtomicInteger mergedInProgress = new AtomicInteger();
    private final Map<HazelcastInstance, UUID> registrations = new HashMap<HazelcastInstance, UUID>();

    MergeBarrier(HazelcastInstance[] instances) {
        MergeCountingListener mergeCountingListener = new MergeCountingListener();

        for (HazelcastInstance instance : instances) {
            LifecycleService lifecycleService = instance.getLifecycleService();
            if (lifecycleService.isRunning()) {
                UUID registration = lifecycleService.addLifecycleListener(mergeCountingListener);
                registrations.put(instance, registration);
            }
        }
    }

    /**
     * Await until there is no merging in progress.
     */
    void awaitNoMergeInProgressAndClose() {
        while (mergedInProgress.get() != 0) {
            HazelcastTestSupport.sleepAtLeastMillis(10);
        }
        close();
    }

    private void close() {
        for (Map.Entry<HazelcastInstance, UUID> entry : registrations.entrySet()) {
            HazelcastInstance instance = entry.getKey();
            UUID registration = entry.getValue();

            instance.getLifecycleService().removeLifecycleListener(registration);
        }
    }

    private class MergeCountingListener implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            LifecycleEvent.LifecycleState state = event.getState();
            if (state.equals(MERGING)) {
                mergedInProgress.incrementAndGet();
            } else if (state.equals(MERGED)) {
                mergedInProgress.decrementAndGet();
            } else if (state.equals(MERGE_FAILED)) {
                mergedInProgress.decrementAndGet();
            }
        }
    }
}
