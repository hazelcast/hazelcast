/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimpleEntryViewTest extends HazelcastTestSupport {

    @Test
    public void test_toString() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, Integer> map = instance.getMap("test");
        map.put(1, 1);

        EntryView entryView = map.getEntryView(1);

        assertEquals(stringify(entryView), entryView.toString());
    }

    private String stringify(EntryView entryView) {
        return "EntryView{"
                + "key=" + entryView.getKey()
                + ", value=" + entryView.getValue()
                + ", cost=" + entryView.getCost()
                + ", creationTime=" + entryView.getCreationTime()
                + ", expirationTime=" + entryView.getExpirationTime()
                + ", hits=" + entryView.getHits()
                + ", lastAccessTime=" + entryView.getLastAccessTime()
                + ", lastStoredTime=" + entryView.getLastStoredTime()
                + ", lastUpdateTime=" + entryView.getLastUpdateTime()
                + ", version=" + entryView.getVersion()
                + ", ttl=" + entryView.getTtl()
                + ", maxIdle=" + entryView.getMaxIdle()
                + '}';
    }
}
