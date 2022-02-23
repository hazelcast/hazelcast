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

import static com.hazelcast.query.impl.extractor.predicates.SingleValueDataStructure.Person;

/**
 * Tests whether all predicates work with the extraction in attributes that are not collections.
 * <p>
 * Extraction mechanism: CUSTOM EXTRACTORS.
 * <p>
 * The trick here is that each extractor is registered under the attribute name like "brain.iq".
 * It is illegal in the production usage, but it enables reusing the test cases from
 * SingleValueAllPredicatesReflectionTest without any code changes, so it is used here.
 * That is the reason why this test may extend the SingleValueAllPredicatesReflectionTest, and the only difference
 * is the registration of the Extractors.
 * <p>
 * This test is parametrised. See SingleValueAllPredicatesReflectionTest for more details.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

                AttributeConfig iqConfig = new TestAttributeIndexConfig();
                iqConfig.setName("brain.iq");
                iqConfig.setExtractorClassName("com.hazelcast.query.impl.extractor.predicates.SingleValueAllPredicatesExtractorTest$IqExtractor");
                mapConfig.addAttributeConfig(iqConfig);

                AttributeConfig nameConfig = new TestAttributeIndexConfig();
                nameConfig.setName("brain.name");
                nameConfig.setExtractorClassName("com.hazelcast.query.impl.extractor.predicates.SingleValueAllPredicatesExtractorTest$NameExtractor");
                mapConfig.addAttributeConfig(nameConfig);
            }
        };
    }

    public static class IqExtractor implements ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.brain.iq);
        }
    }

    public static class NameExtractor implements ValueExtractor<Person, Object> {
        @Override
        public void extract(Person target, Object arguments, ValueCollector collector) {
            collector.addObject(target.brain.name);
        }
    }
}
