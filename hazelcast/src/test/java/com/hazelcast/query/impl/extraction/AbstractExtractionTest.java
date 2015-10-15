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

    private HazelcastInstance instance;
    protected IMap map;

    private InMemoryFormat inMemoryFormat;
    private Index index;

    public AbstractExtractionTest(InMemoryFormat inMemoryFormat, Index index) {
        this.inMemoryFormat = inMemoryFormat;
        this.index = index;
    }

    public abstract static class Configurator {
        public abstract void doWithConfig(Config config);
    }

    public abstract List<String> getIndexedAttributes();

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
            for (String indexedAttribute : getIndexedAttributes()) {
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

    @Parameterized.Parameters(name = "{index}: {0}, {1}")
    public static Collection<Object[]> data() {
        List<Object[]> combinations = new ArrayList<Object[]>();
        for (InMemoryFormat inMemoryFormat : InMemoryFormat.values()) {
            if (inMemoryFormat.equals(InMemoryFormat.NATIVE)) {
                continue; // available only in the enterprise version
            }
            for (Index index : Index.values()) {
                combinations.add(new Object[]{inMemoryFormat, index});
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


}
