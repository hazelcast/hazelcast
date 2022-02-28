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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheIndexConfigTest extends HazelcastTestSupport {

    @Test
    public void testIndexConfigIsRespected() {
        IndexConfig indexConfig = IndexUtils.createTestIndexConfig(IndexType.SORTED, "field");

        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig("map").addQueryCacheConfig(
                new QueryCacheConfig().setName("query-cache").setPredicateConfig(new PredicateConfig(Predicates.alwaysTrue()))
                                      .addIndexConfig(indexConfig));
        config.addMapConfig(mapConfig);

        final HazelcastInstance instance = createHazelcastInstance(config);
        final IMap<Object, Object> map = instance.getMap("map");
        final DefaultQueryCache<Object, Object> cache = (DefaultQueryCache<Object, Object>) map.getQueryCache("query-cache");

        assertNotNull(cache.indexes.getIndex(indexConfig.getName()));
        assertTrue(cache.indexes.getIndex(indexConfig.getName()).isOrdered());
    }

}
