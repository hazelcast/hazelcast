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

package com.hazelcast.jet.stream;

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DistributedStreamTest extends AbstractStreamTest {

    @Test
    public void testProjection() {
        String listName = randomString();
        IMapJet<String, Integer> map = getMap();
        fillMap(map);
        IListJet<String> list = DistributedStream
                .fromMap(map, e -> true, Map.Entry::getKey)
                .collect(DistributedCollectors.toIList(listName));

        assertTrue(list.contains("key-0"));
    }

    @Test
    public void testPredicate() {
        String mapName = randomString();
        IMapJet<String, Integer> map = getMap();
        fillMap(map);
        IMapJet<String, Integer> filteredMap = DistributedStream
                .fromMap(map, e -> !e.getValue().equals(0), wholeItem())
                .collect(DistributedCollectors.toIMap(mapName));

        assertEquals(COUNT - 1, filteredMap.size());
        assertNull(filteredMap.get("key-0"));
    }

}
