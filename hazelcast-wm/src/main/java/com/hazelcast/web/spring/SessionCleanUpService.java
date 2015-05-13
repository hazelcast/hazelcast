/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web.spring;

import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.web.DestroySessionEntryProcessor;
import com.hazelcast.web.HazelcastInstanceDelegate;
import com.hazelcast.web.InvalidateSessionAttributesEntryProcessor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * An internal service to clean invalid session items
 * from internal IMap
 */
public class SessionCleanUpService {

    private static final long FIXED_DELAY = 60;
    private static final long FIXED_DELAY1 = 60;
    private final String name;
    private final ScheduledExecutorService executor;
    private final Queue<KeyEntryProcessorPair> failQueue = new ConcurrentLinkedQueue<KeyEntryProcessorPair>();

    public SessionCleanUpService(final String name) {
        this.name = name;
        executor = Executors.newSingleThreadScheduledExecutor(new CleanupThreadFactory());
    }


    public void registerHazelcastInstanceDelegate(final HazelcastInstanceDelegate delegate) {
        executor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                while (failQueue.isEmpty()) {
                    KeyEntryProcessorPair pair = failQueue.poll();
                    if (pair.getProcessor() instanceof DestroySessionEntryProcessor) {
                        try {
                            // executeOnKey, executeOnEntries should work together
                            delegate.executeOnEntries(pair.getMapName(), pair.getProcessor());
                            Boolean destroyed = (Boolean) delegate.executeOnKey(pair.getMapName(),
                                    pair.getKey(), pair.getProcessor());
                            if (destroyed != null && destroyed) {
                                delegate.executeOnEntries(pair.getMapName(),
                                        new InvalidateSessionAttributesEntryProcessor(pair.getKey()));
                            }
                        } catch (Exception e) {
                            // Put this pair in fail queue again, because
                            // executeOnKey,executeOnEntries did not run correctly
                            failQueue.offer(pair);
                        }
                    } else {
                        delegate.executeOnKey(pair.getMapName(), pair.getKey(), pair.getProcessor());
                    }
                }
            }
        }, FIXED_DELAY, FIXED_DELAY1, TimeUnit.SECONDS);
    }

    public void putKeyEntryProcessorPairToFailQueue(String mapName, String key, EntryProcessor entryProcessor) {
        failQueue.offer(new KeyEntryProcessorPair(mapName, key, entryProcessor));
    }

    public void stop() {
        executor.shutdownNow();
    }

    /**
     * Internal ThreadFactory to create cleanup threads
     */
    private class CleanupThreadFactory implements ThreadFactory {

        public Thread newThread(final Runnable r) {
            final Thread thread = new CleanupThread(r, name + ".hazelcast-wm.cleanup");
            thread.setDaemon(true);
            return thread;
        }
    }

    /**
     * Runnable thread adapter to capture exceptions and notify Hazelcast about them
     */
    private static final class CleanupThread extends Thread {

        private CleanupThread(final Runnable target, final String name) {
            super(target, name);
        }

        public void run() {
            try {
                super.run();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            }
        }
    }

    private class KeyEntryProcessorPair {

        private final String mapName;
        private final String key;
        private final EntryProcessor processor;

        public KeyEntryProcessorPair(String mapName, String key, EntryProcessor processor) {
            this.key = key;
            this.processor = processor;
            this.mapName = mapName;
        }

        public String getKey() {
            return key;
        }

        public String getMapName() {
            return mapName;
        }

        public EntryProcessor getProcessor() {
            return processor;
        }

    }
}
