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

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collection;

import static com.hazelcast.query.Predicates.equal;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractorsAndIndexesTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Test
    public void testExtractorsAreRespectedByEntriesReturnedFromIndexes() {
        String mapName = randomMapName();

        Config config = new Config();
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat)
            .addIndexConfig(new IndexConfig(IndexType.SORTED, "last"))
            .addAttributeConfig(new AttributeConfig("generated", Extractor.class.getName()));
        config.getNativeMemoryConfig().setEnabled(true);

        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, Person> map = instance.getMap(mapName);
        populateMap(map);

        // this predicate queries the index
        Predicate lastPredicate = equal("last", "last");

        // this predicate is not indexed and acts on the entries returned from
        // the index which must support extractors otherwise this test will fail
        Predicate alwaysFirst = equal("generated", "first");

        Predicate composed = Predicates.and(lastPredicate, alwaysFirst);

        Collection<Person> values = map.values(composed);
        assertEquals(100, values.size());
    }

    public static class Person implements Serializable {
        public String first;
        public String last;
    }

    public static class Extractor implements ValueExtractor<Person, Void> {
        @SuppressWarnings("unchecked")
        @Override
        public void extract(Person person, Void aVoid, ValueCollector valueCollector) {
            valueCollector.addObject("first");
        }
    }

    private void populateMap(IMap<Integer, Person> map) {
        for (int i = 0; i < 100; i++) {
            Person p = new Person();
            p.first = "first" + i;
            p.last = "last";
            map.put(i, p);
        }
    }

}
