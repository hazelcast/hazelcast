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

package com.hazelcast.internal.serialization.impl.compact.extractor;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.PredicateTestUtils;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Setups HZ instance and map for extraction testing.
 * Enables configuring the HZ Instance through the getInstanceConfigurator() method that the sub-classes may override.
 */
public abstract class AbstractExtractionTest extends HazelcastTestSupport {

    public enum Index {
        NO_INDEX, HASH, BITMAP
    }

    protected IMap<String, Object> map;

    // three parametrisation axes
    private final Index index;

    /**
     * Holder for objects used as input data in the extraction specification test
     */
    protected static class Input {
        Object[] objects;

        public static Input of(Object... objects) {
            Input input = new Input();
            input.objects = objects;
            return input;
        }
    }

    /**
     * Query executed extraction specification test
     */
    protected static class Query {
        Predicate predicate;
        String expression;

        public static Query of(Predicate predicate) {
            Query query = new Query();
            query.expression = PredicateTestUtils.getAttributeName(predicate);
            query.predicate = predicate;
            return query;
        }
    }

    /**
     * Holder for objects used as expected output in the extraction specification test
     */
    protected static class Expected {
        Object[] objects;
        Class aClass;

        public static Expected of(Object... objects) {
            Expected expected = new Expected();
            expected.objects = objects;
            expected.aClass = objects[0].getClass();
            return expected;
        }

        public static Expected empty() {
            Expected expected = new Expected();
            expected.objects = new ComplexTestDataStructure.Person[0];
            return expected;
        }
    }

    // constructor required by JUnit for parametrisation purposes
    public AbstractExtractionTest(Index index) {
        this.index = index;
    }

    /**
     * Sets up the HZ configuration for the given query specification
     */
    private void setup(Query query) {
        Config config = new Config();

        doWithConfig(config);
        setupIndexes(config, query);
        setupInstance(config);
    }

    public void doWithConfig(Config config) {

    }

    /**
     * Configures the HZ indexing according to the test parameters
     */
    private void setupIndexes(Config config, Query query) {
        if (index != Index.NO_INDEX) {
            IndexConfig indexConfig = new IndexConfig();

            for (String column : query.expression.split(",")) {
                indexConfig.addAttribute(column);
            }

            indexConfig.setType(index == Index.HASH ? IndexType.HASH : IndexType.BITMAP);
            config.getMapConfig("map").addIndexConfig(indexConfig);
        }
    }

    /**
     * Initializes the instance and the map used in the tests
     */
    private void setupInstance(Config config) {
        HazelcastInstance instance = createHazelcastInstance(config);
        map = instance.getMap("map");
    }

    private void putTestDataToMap(Object... objects) {
        for (int i = 0; i < objects.length; i++) {
            map.put(String.valueOf(i), objects[i]);
        }
    }

    /**
     * Enables executing specification tests that are 3 lines long, for example
     * <code>
     * execute(
     * Input.of(BOND, KRUEGER),
     * Query.of(Predicates.equal("limbs_[1].fingers_", "knife"), mv),
     * Expected.of(IllegalArgumentException.class)
     * );
     * </code>
     */
    protected void execute(Input input, Query query, Expected expected) {
        // GIVEN
        setup(query);

        // WHEN
        putTestDataToMap(input.objects);
        Collection<?> values = map.values(query.predicate);

        // THEN
        assertThat(values, hasSize(expected.objects.length));

        if (expected.objects.length > 0) {
            assertThat(values, containsInAnyOrder(expected.objects));
        }
    }

    /**
     * Generates combinations of parameters and outputs them in the *horrible* JUnit format
     */
    protected static Collection<Object[]> axes(List<Index> indexes) {
        List<Object[]> combinations = new ArrayList<Object[]>();
        for (Index index : indexes) {
            combinations.add(new Object[]{index});
        }
        return combinations;
    }
}
