package com.hazelcast.query.impl.extraction.predicates;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.extraction.AbstractExtractionTest;

import static com.hazelcast.query.impl.extraction.predicates.CollectionDataStructure.Limb;
import static com.hazelcast.query.impl.extraction.predicates.CollectionDataStructure.Person;

/**
 * Tests whether all predicates work with the extraction in collections.
 * Each predicate is tested with and without the extraction including "any" operator.
 * Extraction with the "any" operator may return multiple results, thus each predicate has to handle it.
 * <p/>
 * Extraction mechanism: CUSTOM EXTRACTORS.
 * <p/>
 * The trick here is that each extractor is registered under the attribute name like "limbs_[1].power".
 * It is illegal in the production usage, but it enables reusing the test cases from
 * CollectionAllPredicatesReflectionTest without any code changes, so it is used here.
 * That is the reason why this test may extend the CollectionAllPredicatesReflectionTest, and the only difference
 * is the registration of the Extractors.
 * <p/>
 * This test is parametrised. See CollectionAllPredicatesReflectionTest for more details.
 */
public class CollectionAllPredicatesExtractorTest extends CollectionAllPredicatesReflectionTest {

    public CollectionAllPredicatesExtractorTest(InMemoryFormat inMemoryFormat, AbstractExtractionTest.Index index, AbstractExtractionTest.Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected AbstractExtractionTest.Configurator getInstanceConfigurator() {
        return new AbstractExtractionTest.Configurator() {
            @Override
            public void doWithConfig(Config config, AbstractExtractionTest.Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig reducedNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                reducedNameAttribute.setName(AbstractExtractionTest.parametrize("limb_[any].name", mv));
                reducedNameAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.predicates.CollectionAllPredicatesExtractorTest$ReducedLimbNameExtractor");
                mapConfig.addMapAttributeConfig(reducedNameAttribute);

                MapAttributeConfig indexOneNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                indexOneNameAttribute.setName(AbstractExtractionTest.parametrize("limb_[1].name", mv));
                indexOneNameAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.predicates.CollectionAllPredicatesExtractorTest$IndexOneLimbNameExtractor");
                mapConfig.addMapAttributeConfig(indexOneNameAttribute);

                MapAttributeConfig reducedPowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                reducedPowerAttribute.setName(AbstractExtractionTest.parametrize("limb_[any].power", mv));
                reducedPowerAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.predicates.CollectionAllPredicatesExtractorTest$ReducedLimbPowerExtractor");
                mapConfig.addMapAttributeConfig(reducedPowerAttribute);

                MapAttributeConfig indexOnePowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                indexOnePowerAttribute.setName(AbstractExtractionTest.parametrize("limb_[1].power", mv));
                indexOnePowerAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.predicates.CollectionAllPredicatesExtractorTest$IndexOneLimbPowerExtractor");
                mapConfig.addMapAttributeConfig(indexOnePowerAttribute);
            }
        };
    }

    public static class IndexOneLimbPowerExtractor extends ValueExtractor<Person> {
        @Override
        public void extract(Person target, ValueCollector collector) {
            collector.collect(target.limbs_list.get(1).power);
        }
    }

    public static class IndexOneLimbNameExtractor extends ValueExtractor<Person> {
        @Override
        public void extract(Person target, ValueCollector collector) {
            collector.collect(target.limbs_list.get(1).name);
        }
    }

    public static class ReducedLimbPowerExtractor extends ValueExtractor<Person> {
        @Override
        public void extract(Person target, ValueCollector collector) {
            for (Limb limb : target.limbs_list) {
                collector.collect(limb.power);
            }
        }
    }

    public static class ReducedLimbNameExtractor extends ValueExtractor<Person> {
        @Override
        public void extract(Person target, ValueCollector collector) {
            for (Limb limb : target.limbs_list) {
                collector.collect(limb.name);
            }
        }
    }


}
