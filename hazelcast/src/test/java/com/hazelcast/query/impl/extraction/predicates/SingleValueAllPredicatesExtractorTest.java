package com.hazelcast.query.impl.extraction.predicates;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.Arguments;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.extraction.AbstractExtractionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.query.impl.extraction.predicates.SingleValueDataStructure.Person;

/**
 * Tests whether all predicates work with the extraction in attributes that are not collections.
 * <p/>
 * Extraction mechanism: CUSTOM EXTRACTORS.
 * <p/>
 * The trick here is that each extractor is registered under the attribute name like "brain.iq".
 * It is illegal in the production usage, but it enables reusing the test cases from
 * SingleValueAllPredicatesReflectionTest without any code changes, so it is used here.
 * That is the reason why this test may extend the SingleValueAllPredicatesReflectionTest, and the only difference
 * is the registration of the Extractors.
 * <p/>
 * This test is parametrised. See SingleValueAllPredicatesReflectionTest for more details.
 */
@RunWith(Parameterized.class)
public class SingleValueAllPredicatesExtractorTest extends SingleValueAllPredicatesReflectionTest {

    public SingleValueAllPredicatesExtractorTest(InMemoryFormat inMemoryFormat, AbstractExtractionTest.Index index, AbstractExtractionTest.Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected AbstractExtractionTest.Configurator getInstanceConfigurator() {
        return new AbstractExtractionTest.Configurator() {
            @Override
            public void doWithConfig(Config config, AbstractExtractionTest.Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig iqConfig = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                iqConfig.setName("brain.iq");
                iqConfig.setExtractor("com.hazelcast.query.impl.extraction.predicates.SingleValueAllPredicatesExtractorTest$IqExtractor");
                mapConfig.addMapAttributeConfig(iqConfig);

                MapAttributeConfig nameConfig = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                nameConfig.setName("brain.name");
                nameConfig.setExtractor("com.hazelcast.query.impl.extraction.predicates.SingleValueAllPredicatesExtractorTest$NameExtractor");
                mapConfig.addMapAttributeConfig(nameConfig);
            }
        };
    }

    public static class IqExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Arguments arguments, ValueCollector collector) {
            collector.addObject(target.brain.iq);
        }
    }

    public static class NameExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Arguments arguments, ValueCollector collector) {
            collector.addObject(target.brain.name);
        }
    }

}
