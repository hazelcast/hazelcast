/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.eviction;

import com.hazelcast.core.EntryView;

/**
 * LRU eviction policy for an {@link com.hazelcast.core.IMap IMap}
 */
public class LRUEvictionPolicy extends MapEvictionPolicy {

    /**
     * LRU eviction policy instance.
     */
    public static final LRUEvictionPolicy INSTANCE = new LRUEvictionPolicy();

    @Override
    public int compare(EntryView entryView1, EntryView entryView2) {
        long lastAccessTime1 = entryView1.getLastAccessTime();
        long lastAccessTime2 = entryView2.getLastAccessTime();
        return (lastAccessTime1 < lastAccessTime2) ? -1 : ((lastAccessTime1 == lastAccessTime2) ? 0 : 1);
    }
}
