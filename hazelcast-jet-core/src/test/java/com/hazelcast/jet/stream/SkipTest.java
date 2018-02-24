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

import com.hazelcast.core.IList;
import org.junit.Test;

import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

public class SkipTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        int skip = 10;
        IList list = streamMap()
                .skip(skip)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(COUNT - skip, list.size());
    }

    @Test
    public void sourceCache() {
        int skip = 10;
        IList list = streamCache()
                .skip(skip)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(COUNT - skip, list.size());
    }

    @Test
    public void intermediateOperation_sourceMap() {
        int skip = 10;
        IList list = streamMap()
                .map(Entry::getValue)
                .skip(skip)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(COUNT - skip, list.size());
    }

    @Test
    public void intermediateOperation_sourceCache() {
        int skip = 10;
        IList list = streamCache()
                .map(Entry::getValue)
                .skip(skip)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(COUNT - skip, list.size());
    }

    @Test
    public void sourceList() {
        int skip = 1024;
        IList<Integer> result = streamList()
                .skip(skip)
                .collect(DistributedCollectors.toIList(randomString()));

        assertEquals(COUNT - skip, result.size());

        for (int i = 0; i < COUNT - skip; i++) {
            assertEquals(i + skip, (int) result.get(i));
        }
    }
}
