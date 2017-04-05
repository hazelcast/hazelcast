/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.Test;

import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

public class NoneMatchTest extends AbstractStreamTest {

    @Test
    public void sourceMap_noneMatchTrue() {
        boolean found = streamMap()
                .noneMatch(m -> m.getValue() > COUNT);

        assertEquals(true, found);
    }

    @Test
    public void sourceCache_noneMatchTrue() {
        boolean found = streamCache()
                .noneMatch(m -> m.getValue() > COUNT);

        assertEquals(true, found);
    }

    @Test
    public void sourceMap_noneMatchFalse() {
        boolean found = streamMap()
                .noneMatch(m -> m.getValue() > COUNT / 2);

        assertEquals(false, found);
    }

    @Test
    public void sourceCache_noneMatchFalse() {
        boolean found = streamCache()
                .noneMatch(m -> m.getValue() > COUNT / 2);

        assertEquals(false, found);
    }

    @Test
    public void intermediateOperation_noneMatchTrue_sourceMap() {
        boolean found = streamMap()
                .map(Entry::getValue)
                .noneMatch(m -> m > COUNT);

        assertEquals(true, found);
    }

    @Test
    public void intermediateOperation_noneMatchTrue_sourceCache() {
        boolean found = streamCache()
                .map(Entry::getValue)
                .noneMatch(m -> m > COUNT);

        assertEquals(true, found);
    }

    @Test
    public void intermediateOperation_noneMatchFalse_sourceMap() {
        boolean found = streamMap()
                .map(Entry::getValue)
                .noneMatch(m -> m > COUNT / 2);

        assertEquals(false, found);
    }

    @Test
    public void intermediateOperation_noneMatchFalse_sourceCache() {
        boolean found = streamCache()
                .map(Entry::getValue)
                .noneMatch(m -> m > COUNT / 2);

        assertEquals(false, found);
    }

    @Test
    public void sourceList_noneMatch() {
        boolean found = streamList()
                .noneMatch(l -> l > COUNT);

        assertEquals(true, found);
    }

}
