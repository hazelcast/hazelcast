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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheXmlConfigWildcardTest extends HazelcastTestSupport {
    @Test
    public void testQueryCacheXmlConfigWithWildCard() {
        HazelcastInstance hazelcastInstance = createInstance();
        IMap<Integer, Integer> map = hazelcastInstance.getMap("testQueryCacheXmlConfigWithWildCardMap1");
        QueryCache<Integer, Integer> queryCache = map.getQueryCache("queryCacheName1");

        assertNotNull(queryCache);
    }

    protected HazelcastInstance createInstance() {
        ClasspathXmlConfig config = new ClasspathXmlConfig("hazelcast-querycache-xml-config-wildcard-test.xml");
        return createHazelcastInstance(config);
    }
}
