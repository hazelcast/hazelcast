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

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AnyMatchTest extends AbstractStreamTest {

    @Test
    public void sourceMap_matchSuccess() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .anyMatch(m -> m.getValue() > COUNT / 2);


        assertTrue(found);
    }

    @Test
    public void sourceMap_matchFail() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .anyMatch(m -> m.getValue() > COUNT);


        assertFalse(found);
    }

    @Test
    public void intermediateOperation_matchSuccess() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .map(Map.Entry::getValue)
                           .anyMatch(m -> m > COUNT / 2);


        assertTrue(found);
    }

    @Test
    public void intermediateOperation_matchFail() {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        boolean found = map.stream()
                           .map(Map.Entry::getValue)
                           .anyMatch(m -> m > COUNT);


        assertFalse(found);
    }

    @Test
    public void sourceList() {
        IStreamList<Integer> list = getList();
        fillList(list);

        boolean found = list.stream()
                            .anyMatch(l -> l > COUNT / 2);

        assertTrue(found);
    }

}
