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

package com.hazelcast.query.impl.extractor;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Setups HZ instance and map for extraction testing.
 * Enables configuring the HZ Instance through the getInstanceConfigurator() method that the sub-classes may override.
 */
public abstract class AbstractExtractionTest extends AbstractExtractionSpecification {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    protected IMap<String, Object> map;

    // three parametrisation axes
    protected Multivalue mv;
    private InMemoryFormat inMemoryFormat;
    private Index index;

    // constructor required by JUnit for parametrisation purposes
    public AbstractExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        this.inMemoryFormat = inMemoryFormat;
        this.index = index;
        this.mv = multivalue;
    }

    /**
     * Instance configurator enables tweaking the Instance config before the instance starts
     */
    public abstract static class Configurator {
        public abstract void doWithConfig(Config config, Multivalue mv);
    }

    /**
     * Method may be overridden in sub-classes to tweak the HZ instance for purposes of each test.
     */
    protected Configurator getInstanceConfigurator() {
        return null;
    }

    /**
     * Sets up the HZ configuration for the given query specification
     */
    private void setup(Query query) {
        Config config = setupMap(getInstanceConfigurator());
        setupIndexes(config, query);
        setupInstance(config);
    }

    /**
     * Configures the map according to the test parameters and executes the custom configurator
     */
    public Config setupMap(Configurator configurator) {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map");
        mapConfig.setInMemoryFormat(inMemoryFormat);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        if (configurator != null) {
            configurator.doWithConfig(config, mv);
        }

        return config;
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

            indexConfig.setType(index == Index.ORDERED ? IndexType.SORTED : IndexType.HASH);
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

    /**
     * The trick here is that each extractor is registered under the attribute name like "brain.iq".
     * It is illegal in the production usage, but it enables reusing the test cases from
     * reflection-based tests without any code changes and to use them with extractors.
     * In this way we avoid the name validation and can reuse the dot names
     */
    public static class TestAttributeIndexConfig extends AttributeConfig {

        private String name;

        @Override
        public AttributeConfig setName(String name) {
            this.name = name;
            return this;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    /**
     * Populates the map with test data
     */
    private void putTestDataToMap(Object... objects) {
        translate(objects);
        for (int i = 0; i < objects.length; i++) {
            map.put(String.valueOf(i), objects[i]);
        }
    }

    private void translate(Object[] input) {
        if (mv == Multivalue.PORTABLE) {
            for (int i = 0; i < input.length; i++) {
                input[i] = translate(input[i]);
            }
        }
    }

    private <T> T translate(T input) {
        if (mv == Multivalue.PORTABLE) {
            if (input instanceof PortableAware) {
                return ((PortableAware) input).getPortable();
            }
        }
        return input;
    }

    protected Predicate equal(String attribute, Comparable value) {
        return Predicates.equal(attribute, translate(value));
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
        Collection<?> values = null;
        try {
            doWithMap();
            putTestDataToMap(input.objects);
            values = map.values(query.predicate);
        } catch (Exception ex) {
            // EXPECT
            if (expected.throwables != null) {
                for (Class throwable : expected.throwables) {
                    if (throwable.equals(ex.getClass())) {
                        return;
                    }
                }
            }
            ex.printStackTrace();
            fail("Unexpected exception " + ex.getClass());
        }

        // THEN
        if (expected.throwables == null) {
            assertThat(values, hasSize(expected.objects.length));
            if (expected.objects.length > 0) {
                translate(expected.objects);
                assertThat(values, containsInAnyOrder(expected.objects));
            }
        }
    }

    protected void doWithMap() {
    }
}
