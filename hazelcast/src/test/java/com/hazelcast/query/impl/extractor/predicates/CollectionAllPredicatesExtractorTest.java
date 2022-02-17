/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
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
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

                AttributeConfig reducedNameAttribute = new TestAttributeIndexConfig();
                reducedNameAttribute.setName(AbstractExtractionTest.parametrize("limb_[any].name", mv));
                reducedNameAttribute.setExtractorClassName("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$ReducedLimbNameExtractor");
                mapConfig.addAttributeConfig(reducedNameAttribute);

                AttributeConfig indexOneNameAttribute = new TestAttributeIndexConfig();
                indexOneNameAttribute.setName(AbstractExtractionTest.parametrize("limb_[1].name", mv));
                indexOneNameAttribute.setExtractorClassName("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$IndexOneLimbNameExtractor");
                mapConfig.addAttributeConfig(indexOneNameAttribute);

                AttributeConfig reducedPowerAttribute = new TestAttributeIndexConfig();
                reducedPowerAttribute.setName(AbstractExtractionTest.parametrize("limb_[any].power", mv));
                reducedPowerAttribute.setExtractorClassName("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$ReducedLimbPowerExtractor");
                mapConfig.addAttributeConfig(reducedPowerAttribute);

                AttributeConfig indexOnePowerAttribute = new TestAttributeIndexConfig();
                indexOnePowerAttribute.setName(AbstractExtractionTest.parametrize("limb_[1].power", mv));
                indexOnePowerAttribute.setExtractorClassName("com.hazelcast.query.impl.extractor.predicates.CollectionAllPredicatesExtractorTest$IndexOneLimbPowerExtractor");
                mapConfig.addAttributeConfig(indexOnePowerAttribute);
            }
        };
    }

    public static class IndexOneLimbPowerExtractor implements ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.limbs_list.get(1).power);
        }
    }

    public static class IndexOneLimbNameExtractor implements ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.limbs_list.get(1).name);
        }
    }

    public static class ReducedLimbPowerExtractor implements ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            for (Limb limb : target.limbs_list) {
                collector.addObject(limb.power);
            }
        }
    }

    public static class ReducedLimbNameExtractor implements ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            for (Limb limb : target.limbs_list) {
                collector.addObject(limb.name);
            }
        }
    }
}
