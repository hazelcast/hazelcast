/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;

public class HazelcastClientIdGeneratorTest extends HazelcastClientTestBase {

    @Test
    public void idGenerator() {
        HazelcastClient hClient = getHazelcastClient();
        HazelcastInstance h = getHazelcastInstance();
        IdGenerator nativeId = h.getIdGenerator("id");
        IdGenerator clientId = hClient.getIdGenerator("id");
        long v = clientId.newId();
        Map<Long, Integer> map = new HashMap<Long, Integer>();
        int count = 10;
        for (int i = 0; i < count; i++) {
            long genId = nativeId.newId();
            assertNull(map.put(genId, 1));
//            System.out.println(genId);
        }
        System.out.println(v);
        for (int i = 0; i < count; i++) {
            long genId = clientId.newId();
            assertNull(map.put(genId, 1));
//            System.out.println(genId);
        }
        assertTrue(true);
    }
}
