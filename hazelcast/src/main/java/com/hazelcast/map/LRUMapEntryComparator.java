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

package com.hazelcast.map;

import com.hazelcast.core.EntryView;

import java.util.Comparator;

public class LRUMapEntryComparator implements Comparator<EntryView> {
    public int compare(EntryView r1, EntryView r2) {
//        final long t1 = Math.max(r1.getLastAccessTime(), Math.max(r1.getLastUpdateTime(), r1.getCreationTime()));
//        final long t2 = Math.max(r2.getLastAccessTime(), Math.max(r2.getLastUpdateTime(), r2.getCreationTime()));
//        return t1 > t2 ? 1 : t1 == t2 ? 0 : -1;
        return 0;
    }
}
