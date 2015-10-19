package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.MultiResult;
import com.hazelcast.query.extractor.ValueExtractor;

import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.Limb;
import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.Person;

public class MultipleCollectionsExtractorExtractionTest extends MultipleCollectionsReflectionExtractionTest {

    public MultipleCollectionsExtractorExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected Configurator getInstanceConfigurator() {
        return new Configurator() {
            @Override
            public void doWithConfig(Config config, AbstractExtractionTest.Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig reducedNameAttribute = new TestMapAttributeIndexConfig();
                reducedNameAttribute.setName(parametrize("limbs_[1].fingers_[1]", mv));
                reducedNameAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.MultipleCollectionsExtractorExtractionTest$BothIndexOneExtractor");
                mapConfig.addMapAttributeConfig(reducedNameAttribute);

                MapAttributeConfig indexOneNameAttribute = new TestMapAttributeIndexConfig();
                indexOneNameAttribute.setName(parametrize("limbs_[any].fingers_[any]", mv));
                indexOneNameAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.MultipleCollectionsExtractorExtractionTest$BothReducedExtractor");
                mapConfig.addMapAttributeConfig(indexOneNameAttribute);

                MapAttributeConfig reducedPowerAttribute = new TestMapAttributeIndexConfig();
                reducedPowerAttribute.setName(parametrize("limbs_[any].fingers_[1]", mv));
                reducedPowerAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.MultipleCollectionsExtractorExtractionTest$FirstReducedExtractor");
                mapConfig.addMapAttributeConfig(reducedPowerAttribute);

                MapAttributeConfig indexOnePowerAttribute = new TestMapAttributeIndexConfig();
                indexOnePowerAttribute.setName(parametrize("limbs_[1].fingers_[any]", mv));
                indexOnePowerAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.MultipleCollectionsExtractorExtractionTest$LastReducedExtractor");
                mapConfig.addMapAttributeConfig(indexOnePowerAttribute);
            }
        };
    }

    public static class BothIndexOneExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).limbs_list.get(1).fingers_array[1];
        }
    }

    public static class BothReducedExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs_list) {
                for (String finger : limb.fingers_list) {
                    multiResult.add(finger);
                }
            }
            return multiResult;
        }
    }

    public static class FirstReducedExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs_list) {
                multiResult.add(limb.fingers_array[1]);
            }
            return multiResult;
        }
    }

    public static class LastReducedExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            Limb limb = ((Person) target).limbs_list.get(1);
            for (String finger : limb.fingers_array) {
                multiResult.add(finger);
            }
            return multiResult;
        }
    }

}
