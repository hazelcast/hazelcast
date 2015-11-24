package com.hazelcast.query.impl.extractor;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Collection;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Setups HZ instance and map for extraction testing.
 * Enables configuring the HZ Instance through the getInstanceConfigurator() method that the sub-classes may override.
 */
public abstract class AbstractExtractionTest extends AbstractExtractionSpecification {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    protected IMap<String, Object> map;

    // three parametrisation axes
    private InMemoryFormat inMemoryFormat;
    private Index index;
    protected Multivalue mv;

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
            MapIndexConfig mapIndexConfig = new MapIndexConfig();
            mapIndexConfig.setAttribute(query.expression);
            mapIndexConfig.setOrdered(index == Index.ORDERED);
            config.getMapConfig("map").addMapIndexConfig(mapIndexConfig);
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
    public static class TestMapAttributeIndexConfig extends MapAttributeConfig {
        private String name;

        public MapAttributeConfig setName(String name) {
            this.name = name;
            return this;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * Populates the map with test data
     */
    private void putTestDataToMap(Object... objects) {
        int i = 0;
        for (Object person : objects) {
            map.put(String.valueOf(i++), person);
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

        // EXPECT
        if (expected.throwable != null) {
            this.expected.expect(expected.throwable);
        }

        // WHEN
        putTestDataToMap(input.objects);
        Collection<?> values = map.values(query.predicate);

        // THEN
        if (expected.throwable == null) {
            assertThat(values, hasSize(expected.objects.length));
            if (expected.objects.length > 0) {
                assertThat(values, containsInAnyOrder(expected.objects));
            }
        }
    }
}
