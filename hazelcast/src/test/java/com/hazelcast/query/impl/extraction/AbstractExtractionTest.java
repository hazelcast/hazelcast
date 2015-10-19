package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractExtractionTest extends HazelcastTestSupport {

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
    protected Multivalue multivalue;

    public AbstractExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        this.inMemoryFormat = inMemoryFormat;
        this.index = index;
        this.multivalue = multivalue;
    }

    public abstract static class Configurator {
        public abstract void doWithConfig(Config config);
    }

    /**
     * @return Attribute names to be indexed
     */
    public abstract List<String> indexAttributes();

    public void setup() {
        setup(null);
    }

    public void setup(Configurator configurator) {
        if (instance != null) {
            instance.shutdown();
        }

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map");
        mapConfig.setInMemoryFormat(inMemoryFormat);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        if (index != Index.NO_INDEX) {
            for (String indexedAttribute : indexAttributes()) {
                MapIndexConfig mapIndexConfig = new MapIndexConfig();
                mapIndexConfig.setAttribute(indexedAttribute);
                mapIndexConfig.setOrdered(index == Index.ORDERED);
                mapConfig.addMapIndexConfig(mapIndexConfig);
            }
        }

        if (configurator != null) {
            configurator.doWithConfig(config);
        }
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

    protected Configurator getConfigurator() {
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

    static class Input {
        Object[] persons;

        static Input of(Object... objects) {
            Input input = new Input();
            input.persons = objects;
            return input;
        }
    }

    static class Query {
        String expression;
        Comparable input;

        static Query of(String exp, Comparable input, Multivalue multivalue) {
            Query query = new Query();
            // add _array[ or _list[ prefix
            query.expression = exp.replaceAll("_", "_" + multivalue.name().toLowerCase());
            query.input = input;
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


}
