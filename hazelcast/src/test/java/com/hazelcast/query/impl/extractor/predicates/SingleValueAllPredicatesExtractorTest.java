/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.query.impl.extractor.predicates.SingleValueDataStructure.Person;

/**
 * Tests whether all predicates work with the extraction in attributes that are not collections.
 *
 * Extraction mechanism: CUSTOM EXTRACTORS.
 *
 * The trick here is that each extractor is registered under the attribute name like "brain.iq".
 * It is illegal in the production usage, but it enables reusing the test cases from
 * SingleValueAllPredicatesReflectionTest without any code changes, so it is used here.
 * That is the reason why this test may extend the SingleValueAllPredicatesReflectionTest, and the only difference
 * is the registration of the Extractors.
 *
 * This test is parametrised. See SingleValueAllPredicatesReflectionTest for more details.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("unused")
public class SingleValueAllPredicatesExtractorTest extends SingleValueAllPredicatesReflectionTest {

    public SingleValueAllPredicatesExtractorTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Override
    protected AbstractExtractionTest.Configurator getInstanceConfigurator() {
        return new AbstractExtractionTest.Configurator() {
            @Override
            public void doWithConfig(Config config, Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig iqConfig = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                iqConfig.setName("brain.iq");
                iqConfig.setExtractor("com.hazelcast.query.impl.extractor.predicates.SingleValueAllPredicatesExtractorTest$IqExtractor");
                mapConfig.addMapAttributeConfig(iqConfig);

                MapAttributeConfig nameConfig = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                nameConfig.setName("brain.name");
                nameConfig.setExtractor("com.hazelcast.query.impl.extractor.predicates.SingleValueAllPredicatesExtractorTest$NameExtractor");
                mapConfig.addMapAttributeConfig(nameConfig);
            }
        };
    }

    public static class IqExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.brain.iq);
        }
    }

    public static class NameExtractor extends ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.brain.name);
        }
    }
}
