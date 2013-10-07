/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapIndexFactoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class IndexFactoryTest extends HazelcastTestSupport {

    static class Pojo implements Serializable {
        String text;

        Pojo() {
        }

        Pojo(String text) {
            this.text = text;
        }
    }

    static class PojoIndex implements Index {

        private final String attributeName;
        private final boolean ordered;
        private final Set<QueryableEntry> set;
        private final String value;

        PojoIndex(boolean ordered, String attributeName, Set<QueryableEntry> set, String value) {
            this.set = set;
            this.attributeName = attributeName;
            this.ordered = ordered;
            this.value = value;
        }

        @Override
        public void saveEntryIndex(QueryableEntry e) throws QueryException {
            set.add(e);
        }

        @Override
        public void clear() {
            set.clear();
        }

        @Override
        public void removeEntryIndex(Data indexKey) {
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable[] values) {
            return set;
        }

        @Override
        public Set<QueryableEntry> getRecords(Comparable value) {
            final Set<QueryableEntry> result = new HashSet<QueryableEntry>();
            for (final QueryableEntry qe : set) {
                if (qe.getAttribute(attributeName).compareTo(value) == 0) {
                    result.add(qe);
                }
            }
            return result;
        }

        @Override
        public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
            return set;
        }

        @Override
        public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
            return set;
        }

        @Override
        public String getAttributeName() {
            return attributeName;
        }

        @Override
        public boolean isOrdered() {
            return ordered;
        }
    }

    @Test
    public void testIndexFactoryImplementation() {

        final Set<QueryableEntry> set = new HashSet<QueryableEntry>();
        MapIndexFactoryConfig mapIndexFactoryConfig = new MapIndexFactoryConfig();
        mapIndexFactoryConfig.setFactoryImplementation(new IndexFactory() {
            @Override
            public Index createIndex(String attribute, boolean ordered, Properties properties) {
                return "text".equals(attribute) ? new PojoIndex(true, "text", set, "5") : null;
            }
        });
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        Config config = new Config();

        config.getMapConfig("testIndexFactory").setMapIndexFactoryConfig(mapIndexFactoryConfig);

        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);

        IMap map = instance.getMap("testIndexFactory");
        map.addIndex("text", true);
        for (int i = 0; i < 10; i++) {
            map.put(i, new Pojo(Integer.toString(i)));
        }
        final Collection values = map.values(Predicates.equal("text", "5"));
        assertEquals(10, set.size());
        assertEquals(1, values.size());
    }
}
