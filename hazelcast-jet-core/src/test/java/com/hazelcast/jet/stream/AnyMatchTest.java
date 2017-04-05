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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AnyMatchTest extends AbstractStreamTest {

    @Test
    public void sourceMap_matchSuccess() {
        boolean found = streamMap().anyMatch(m -> m.getValue() > COUNT / 2);

        assertTrue(found);
    }

    @Test
    public void sourceCache_matchSuccess() {
        boolean found = streamCache().anyMatch(m -> m.getValue() > COUNT / 2);

        assertTrue(found);
    }

    @Test
    public void sourceMap_matchFail() {
        boolean found = streamMap().anyMatch(m -> m.getValue() > COUNT);

        assertFalse(found);
    }

    @Test
    public void sourceCache_matchFail() {
        boolean found = streamCache().anyMatch(m -> m.getValue() > COUNT);

        assertFalse(found);
    }

    @Test
    public void intermediateOperation_sourceMap_matchSuccess() {
        boolean found = streamMap()
                .map(Entry::getValue)
                .anyMatch(m -> m > COUNT / 2);

        assertTrue(found);
    }

    @Test
    public void intermediateOperation_sourceCache_matchSuccess() {
        boolean found = streamCache()
                .map(Entry::getValue)
                .anyMatch(m -> m > COUNT / 2);

        assertTrue(found);
    }

    @Test
    public void intermediateOperation_sourceMap_matchFail() {
        boolean found = streamMap()
                .map(Entry::getValue)
                .anyMatch(m -> m > COUNT);

        assertFalse(found);
    }

    @Test
    public void intermediateOperation_sourceCache_matchFail() {
        boolean found = streamCache()
                .map(Entry::getValue)
                .anyMatch(m -> m > COUNT);

        assertFalse(found);
    }

    @Test
    public void sourceList() {
        boolean found = streamList()
                .anyMatch(l -> l > COUNT / 2);

        assertTrue(found);
    }

}
