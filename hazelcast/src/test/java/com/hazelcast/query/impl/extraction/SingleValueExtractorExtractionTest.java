package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.ValueExtractor;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.query.impl.extraction.SingleValueDataStructure.Person;


@RunWith(Parameterized.class)
public class SingleValueExtractorExtractionTest extends SingleValueReflectionExtractionTest {

    public SingleValueExtractorExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected Configurator getInstanceConfigurator() {
        return new Configurator() {
            @Override
            public void doWithConfig(Config config, AbstractExtractionTest.Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig iqConfig = new TestMapAttributeIndexConfig();
                iqConfig.setName("brain.iq");
                iqConfig.setExtractor("com.hazelcast.query.impl.extraction.SingleValueExtractorExtractionTest$IqExtractor");
                mapConfig.addMapAttributeConfig(iqConfig);

                MapAttributeConfig nameConfig = new TestMapAttributeIndexConfig();
                nameConfig.setName("brain.name");
                nameConfig.setExtractor("com.hazelcast.query.impl.extraction.SingleValueExtractorExtractionTest$NameExtractor");
                mapConfig.addMapAttributeConfig(nameConfig);
            }
        };
    }

    public static class IqExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).brain.iq;
        }
    }

    public static class NameExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).brain.name;
        }
    }

}
