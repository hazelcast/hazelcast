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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeIndexXmlConfigTest extends HazelcastTestSupport {

    IMap<String, Integer> map;

    @Before
    public void before() {
        map = createHazelcastInstance().getMap("map");
        for (int i = 0; i < 100; ++i) {
            map.put(Integer.toString(i), i);
        }
    }

    @Override
    protected Config getConfig() {
        return new ClasspathXmlConfig("hazelcast-composite-index-xml-config-test.xml");
    }

    @Test
    public void test() {
        Collection<Integer> result = map.values(Predicates.sql("__key = '10' and __key.length = 2"));
        assertEquals(1, result.size());
        assertEquals((Integer) 10, result.iterator().next());
        assertEquals(1, map.getLocalMapStats().getIndexStats().get("testIndex").getQueryCount());
    }

}
