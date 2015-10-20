package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.AbstractPredicate;
import com.hazelcast.query.impl.predicates.PredicateTestUtils;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public abstract class AbstractExtractionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();


    enum Index {
        NO_INDEX, UNORDERED, ORDERED
    }

    enum Multivalue {
        ARRAY,
        LIST
    }

    private HazelcastInstance instance;
    protected IMap map;

    private InMemoryFormat inMemoryFormat;
    private Index index;
    protected Multivalue mv;

    public AbstractExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        this.inMemoryFormat = inMemoryFormat;
        this.index = index;
        this.mv = multivalue;
    }

    public abstract static class Configurator {
        public abstract void doWithConfig(Config config, Multivalue mv);
    }

    public void setupMap() {
        setupMap(null);
    }

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

    private void setupIndexes(Config config, Query query) {
        if (index != Index.NO_INDEX) {
            MapIndexConfig mapIndexConfig = new MapIndexConfig();
            mapIndexConfig.setAttribute(query.expression);
            mapIndexConfig.setOrdered(index == Index.ORDERED);
            config.getMapConfig("map").addMapIndexConfig(mapIndexConfig);
        }
    }

    private void setupInstance(Config config) {
        instance = createHazelcastInstance(config);
        map = instance.getMap("map");
    }


    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> data() {
        List<Object[]> combinations = new ArrayList<Object[]>();
        for (InMemoryFormat inMemoryFormat : InMemoryFormat.values()) {
            if (inMemoryFormat.equals(InMemoryFormat.NATIVE)) {
                continue; // available only in the enterprise version
            }
            for (Index index : Index.values()) {
                for (Multivalue multivalue : Multivalue.values()) {
                    combinations.add(new Object[]{inMemoryFormat, index, multivalue});
                }
            }
        }
        return combinations;
    }

    protected Configurator getInstanceConfigurator() {
        return null;
    }

    // this way we avoid the name validation and can reuse the dot names
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

    //
    // TEST EXECUTION UTILITIES
    //
    static class Input {
        Object[] objects;

        static Input of(Object... objects) {
            Input input = new Input();
            input.objects = objects;
            return input;
        }
    }

    static class Query {
        AbstractPredicate predicate;
        String expression;

        static Query of(Predicate predicate, Multivalue mv) {
            AbstractPredicate ap = (AbstractPredicate) predicate;
            Query query = new Query();
            query.expression = parametrize(PredicateTestUtils.getAttributeName(ap), mv);
            PredicateTestUtils.setAttributeName((AbstractPredicate) predicate, query.expression);
            query.predicate = ap;
            return query;
        }
    }

    static class Expected {
        Object[] objects;
        Class<? extends Throwable> throwable;

        static Expected of(Object... objects) {
            Expected expected = new Expected();
            expected.objects = objects;
            return expected;
        }

        static Expected of(Class<? extends Throwable> throwable) {
            Expected expected = new Expected();
            expected.throwable = throwable;
            return expected;
        }

        static Expected empty() {
            Expected expected = new Expected();
            expected.objects = new ComplexCaseDataStructure.Person[0];
            return expected;
        }
    }

    private void putTestDataToMap(Object... objects) {
        int i = 0;
        for (Object person : objects) {
            map.put(String.valueOf(i++), person);
        }
    }

    protected void execute(Input input, Query query, Expected expected) {
        // GIVEN
        Config config = setupMap(getInstanceConfigurator());
        setupIndexes(config, query);
        setupInstance(config);

        // EXPECT
        if (expected.throwable != null) {
            this.expected.expect(expected.throwable);
        }

        putTestDataToMap(input.objects);

        // WHEN
        Collection<?> values = map.values(query.predicate);

        // THEN
        if (expected.throwable == null) {
            assertThat(values, hasSize(expected.objects.length));
            if (expected.objects.length > 0) {
                assertThat(values, containsInAnyOrder(expected.objects));
            }
        }
    }

    static String parametrize(String expression, AbstractExtractionTest.Multivalue mv) {
        if (!expression.contains("__")) {
            return expression.replaceAll("_", "_" + mv.name().toLowerCase());
        }
        return expression;
    }

}
