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
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

public class ReduceTest extends AbstractStreamTest {

    @Test
    public void testReduceWithIdentity() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        int result = map.stream()
                        .map(Entry::getValue)
                        .reduce(0, (left, right) -> left + right);

        assertEquals((COUNT - 1) * (COUNT) / 2, result);
    }

    @Test
    public void testReduceWithNoIdentity() throws Exception {
        IStreamMap<String, Integer> map = getMap();
        fillMap(map);

        int result = map.stream()
                        .map(Map.Entry::getValue)
                        .reduce((left, right) -> left + right).get();

        assertEquals((COUNT - 1) * (COUNT) / 2, result);
    }

}
