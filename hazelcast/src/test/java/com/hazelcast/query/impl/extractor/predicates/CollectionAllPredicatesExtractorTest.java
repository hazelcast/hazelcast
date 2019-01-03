/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.extractor.predicates;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.query.impl.extractor.predicates.CollectionDataStructure.Limb;
import static com.hazelcast.query.impl.extractor.predicates.CollectionDataStructure.Person;

/**
 * Tests whether all predicates work with the extraction in collections.
 * Each predicate is tested with and without the extraction including "any" operator.
 * Extraction with the "any" operator may return multiple results, thus each predicate has to handle it.
 * <p>
 * Extraction mechanism: CUSTOM EXTRACTORS.
 * <p>
 * The trick here is that each extractor is registered under the attribute name like "limbs_[1].power".
 * It is illegal in the production usage, but it enables reusing the test cases from
 * CollectionAllPredicatesReflectionTest without any code changes, so it is used here.
 * That is the reason why this test may extend the CollectionAllPredicatesReflectionTest, and the only difference
 * is the registration of the Extractors.
 * <p>
 * This test is parametrised. See CollectionAllPredicatesReflectionTest for more details.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("unused")
public class CollectionAllPredicatesExtractorTest extends CollectionAllPredicatesReflectionTest {

    public CollectionAllPredicatesExtractorTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Override
    protected AbstractExtractionTest.Configurator getInstanceConfigurator() {
        return new AbstractExtractionTest.Configurator() {
            @Override
            public void doWithConfig(Config config, Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig reducedNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                reducedNameAttribute.setName(AbstractExtractionTest.parametrize("limb_[any].name", mv));
                reducedNameAttribute.setExtractor("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$ReducedLimbNameExtractor");
                mapConfig.addMapAttributeConfig(reducedNameAttribute);

                MapAttributeConfig indexOneNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                indexOneNameAttribute.setName(AbstractExtractionTest.parametrize("limb_[1].name", mv));
                indexOneNameAttribute.setExtractor("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$IndexOneLimbNameExtractor");
                mapConfig.addMapAttributeConfig(indexOneNameAttribute);

                MapAttributeConfig reducedPowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                reducedPowerAttribute.setName(AbstractExtractionTest.parametrize("limb_[any].power", mv));
                reducedPowerAttribute.setExtractor("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$ReducedLimbPowerExtractor");
                mapConfig.addMapAttributeConfig(reducedPowerAttribute);

                MapAttributeConfig indexOnePowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                indexOnePowerAttribute.setName(AbstractExtractionTest.parametrize("limb_[1].power", mv));
                indexOnePowerAttribute.setExtractor("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$IndexOneLimbPowerExtractor");
                mapConfig.addMapAttributeConfig(indexOnePowerAttribute);
            }
        };
    }

    public static class IndexOneLimbPowerExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.limbs_list.get(1).power);
        }
    }

    public static class IndexOneLimbNameExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.limbs_list.get(1).name);
        }
    }

    public static class ReducedLimbPowerExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            for (Limb limb : target.limbs_list) {
                collector.addObject(limb.power);
            }
        }
    }

    public static class ReducedLimbNameExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            for (Limb limb : target.limbs_list) {
                collector.addObject(limb.name);
            }
        }
    }
}
