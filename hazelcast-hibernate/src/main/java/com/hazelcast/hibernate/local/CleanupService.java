/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.hibernate.local;

import com.hazelcast.impl.OutOfMemoryErrorDispatcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 11/14/12
 */
public final class CleanupService {

    private final ScheduledExecutorService executor;

    public CleanupService(final String name) {
        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            public Thread newThread(final Runnable r) {
                return new Thread(r, name + ".hibernate.cleanup") {
                    public void run() {
                        try {
                            super.run();
                        } catch (OutOfMemoryError e) {
                            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                        }
                    }
                };
            }
        });
    }

    public void registerCache(final LocalRegionCache cache) {
        executor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                cache.cleanup();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdownNow();
    }
}
