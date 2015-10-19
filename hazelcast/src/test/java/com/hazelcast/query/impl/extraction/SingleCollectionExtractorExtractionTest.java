package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.MultiResult;
import com.hazelcast.query.extractor.ValueExtractor;

import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.Limb;
import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.Person;

public class SingleCollectionExtractorExtractionTest extends SingleCollectionReflectionExtractionTest {

    public SingleCollectionExtractorExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected Configurator getInstanceConfigurator() {
        return new AbstractExtractionTest.Configurator() {
            @Override
            public void doWithConfig(Config config, AbstractExtractionTest.Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig reducedNameAttribute = new TestMapAttributeIndexConfig();
                reducedNameAttribute.setName(parametrize("limb_[any].name", mv));
                reducedNameAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.SingleCollectionExtractorExtractionTest$ReducedLimbNameExtractor");
                mapConfig.addMapAttributeConfig(reducedNameAttribute);

                MapAttributeConfig indexOneNameAttribute = new TestMapAttributeIndexConfig();
                indexOneNameAttribute.setName(parametrize("limb_[1].name", mv));
                indexOneNameAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.SingleCollectionExtractorExtractionTest$IndexOneLimbNameExtractor");
                mapConfig.addMapAttributeConfig(indexOneNameAttribute);

                MapAttributeConfig reducedPowerAttribute = new TestMapAttributeIndexConfig();
                reducedPowerAttribute.setName(parametrize("limb_[any].power", mv));
                reducedPowerAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.SingleCollectionExtractorExtractionTest$ReducedLimbPowerExtractor");
                mapConfig.addMapAttributeConfig(reducedPowerAttribute);

                MapAttributeConfig indexOnePowerAttribute = new TestMapAttributeIndexConfig();
                indexOnePowerAttribute.setName(parametrize("limb_[1].power", mv));
                indexOnePowerAttribute.setExtractor(
                        "com.hazelcast.query.impl.extraction.SingleCollectionExtractorExtractionTest$IndexOneLimbPowerExtractor");
                mapConfig.addMapAttributeConfig(indexOnePowerAttribute);
            }
        };
    }

    public static class IndexOneLimbPowerExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).limbs_list.get(1).power;
        }
    }

    public static class IndexOneLimbNameExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).limbs_list.get(1).name;
        }
    }

    public static class ReducedLimbPowerExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs_list) {
                multiResult.add(limb.power);
            }
            return multiResult;
        }
    }

    public static class ReducedLimbNameExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs_list) {
                multiResult.add(limb.name);
            }
            return multiResult;
        }
    }


}
