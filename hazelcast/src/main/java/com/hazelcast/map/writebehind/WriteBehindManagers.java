/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.writebehind;

import com.hazelcast.map.MapService;
import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.map.writebehind.store.StoreListener;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A class providing static factory methods that create write behind managers.
 */
public final class WriteBehindManagers {

    private WriteBehindManagers() {
    }

    public static WriteBehindManager createWriteBehindManager(String mapName,
                                                              MapService service, MapStoreWrapper storeWrapper) {
        return new WriteBehindQueueManager(mapName, service, storeWrapper);
    }

    public static WriteBehindManager emptyWriteBehindManager() {
        return EmptyWriteBehindQueueManagerHolder.EMPTY_WRITE_BEHIND_QUEUE_MANAGER;
    }

    /**
     * Holder provides lazy initialization for singleton instance.
     */
    private static final class EmptyWriteBehindQueueManagerHolder {
        /**
         * Empty manager.
         */
        private static final WriteBehindManager EMPTY_WRITE_BEHIND_QUEUE_MANAGER = new EmptyWriteBehindQueueManager();
    }

    /**
     * Empty write behind queue manager provides neutral null behaviour.
     */
    private static class EmptyWriteBehindQueueManager implements WriteBehindManager {

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public void reset() {

        }

        @Override
        public void addStoreListener(StoreListener storeListener) {
        }

        @Override
        public ScheduledExecutorService getScheduler() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Data> flush(WriteBehindQueue queue) {
            return Collections.emptyList();
        }

        // Preserves singleton property
        private Object readResolve() {
            return EmptyWriteBehindQueueManagerHolder.EMPTY_WRITE_BEHIND_QUEUE_MANAGER;
        }
    }
}
