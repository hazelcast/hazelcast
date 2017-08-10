package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGE_FAILED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;

/**
 * Protects against in-progress merging.
 *
 *
 */
class MergeBarrier {
    private final AtomicInteger mergedInProgress = new AtomicInteger();
    private final Map<HazelcastInstance, String> registrations = new HashMap<HazelcastInstance, String>();

    MergeBarrier(HazelcastInstance[] instances) {
        MergeCountingListener mergeCountingListener = new MergeCountingListener();

        for (HazelcastInstance instance : instances) {
            LifecycleService lifecycleService = instance.getLifecycleService();
            if (lifecycleService.isRunning()) {
                String registeration = lifecycleService.addLifecycleListener(mergeCountingListener);
                registrations.put(instance, registeration);
            }
        }
    }

    /**
     * Await until there is no merging in progress.
     *
     */
    void awaitNoMergeInProgressAndClose() {
        while (mergedInProgress.get() != 0) {
            HazelcastTestSupport.sleepAtLeastMillis(10);
        }
        close();
    }

    private void close() {
        for (Map.Entry<HazelcastInstance, String> entry : registrations.entrySet()) {
            HazelcastInstance instance = entry.getKey();
            String registration = entry.getValue();

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
