/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.core.MapEntry;
import com.hazelcast.query.TestUtil;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class LFUMapEntryComparatorTest extends TestUtil {

    @Test
    public void testLFU() {
        List<MapEntry> lsEntries = new ArrayList<MapEntry>();
        Map<Long, EmptyMapEntry> mapEntries = new HashMap<Long, EmptyMapEntry>();
        for (int i = 0; i < 10; i++) {
            EmptyMapEntry e = new EmptyMapEntry(i);
            lsEntries.add(e);
            mapEntries.put(e.getId(), e);
        }
        assertEquals (10, lsEntries.size());
        Collections.shuffle(lsEntries);
        assertEquals (10, lsEntries.size());
        for (int i = 0; i < 10; i++) {
            EmptyMapEntry entry = mapEntries.get(Long.valueOf(i));
            entry.setHits(i);
        }
        Set<MapEntry> sorted = new TreeSet<MapEntry>(new LFUMapEntryComparator());
        sorted.addAll(lsEntries);
        assertEquals (10, sorted.size());
        long count = 0;
        for (MapEntry e : sorted) {
            EmptyMapEntry entry = (EmptyMapEntry) e;
            assertEquals(entry.getId(), count);
            count++;
        }
    }
}
