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

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsupportedBitmapIndexPredicatesTest extends HazelcastTestSupport {

    private IMap<Integer, Integer> map;

    @Before
    public void before() {
        Config config = smallInstanceConfig();
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        config.getMapConfig("map").addIndexConfig(new IndexConfig(IndexType.BITMAP, "this"));
        map = createHazelcastInstance(config).getMap("map");
        for (int i = 0; i < 10; ++i) {
            map.put(i, i);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnsupportedPredicate() {
        Collection<Integer> values = map.values(new UnsupportedPredicate("this", 5));
        assertEquals(1, values.size());
        assertEquals(5, (int) values.iterator().next());
        assertFalse(UnsupportedPredicate.filterInvoked);
    }

    private static class UnsupportedPredicate extends EqualPredicate {

        static volatile boolean filterInvoked;

        UnsupportedPredicate(String attribute, Comparable value) {
            super(attribute, value);
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            filterInvoked = true;
            return super.filter(queryContext);
        }

    }

}
