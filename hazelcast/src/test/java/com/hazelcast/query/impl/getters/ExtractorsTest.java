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

package com.hazelcast.query.impl.getters;

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.query.impl.getters.GetterCache.SIMPLE_GETTER_CACHE_SUPPLIER;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class ExtractorsTest {

    @Parameters(name = "useClassloader:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Parameter
    public boolean useClassloader;

    private Bond bond = new Bond();

    private InternalSerializationService ss;

    @Before
    public void setUp() throws Exception {
        DefaultSerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        ss = builder.setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void when_getGetterByReflection_then_getterInCache() {
        // GIVEN
        Extractors extractors = createExtractors(null);

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(bond, "car.power", true);
        Getter getterSecondInvocation = extractors.getGetter(bond, "car.power", true);

        // THEN
        assertThat(getterFirstInvocation, sameInstance(getterSecondInvocation));
        assertThat(getterFirstInvocation, instanceOf(FieldGetter.class));
    }

    @Test
    public void when_extractByReflection_then_correctValue() {
        // WHEN
        Object power = createExtractors(null).extract(bond, "car.power", null);

        // THEN
        assertThat((Integer) power, equalTo(550));
    }

    @Test
    public void when_getGetterExtractor_then_getterInCacheWithProperType() {
        // GIVEN
        AttributeConfig config
                = new AttributeConfig("gimmePower", "com.hazelcast.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = createExtractors(config);

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(bond, "gimmePower", true);
        Getter getterSecondInvocation = extractors.getGetter(bond, "gimmePower", true);

        // THEN
        assertThat(getterFirstInvocation, sameInstance(getterSecondInvocation));
        assertThat(getterFirstInvocation, instanceOf(ExtractorGetter.class));
    }

    @Test
    public void when_extractExtractor_then_correctValue() {
        // GIVEN
        AttributeConfig config
                = new AttributeConfig("gimmePower", "com.hazelcast.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = createExtractors(config);

        // WHEN
        Object power = extractors.extract(bond, "gimmePower", null);

        // THEN
        assertThat((Integer) power, equalTo(550));
    }

    @Test
    public void when_extractWithNullTarget_then_nullValue() {
        // WHEN
        Object power = createExtractors(null).extract(null, "gimmePower", null);

        // THEN
        assertNull(power);
    }

    @Test
    public void when_extractWithNullParams_then_nullValue() {
        // WHEN
        Object power = createExtractors(null).extract(null, null, null);

        // THEN
        assertNull(power);
    }

    @Test(expected = NullPointerException.class)
    public void when_extractWithNullAttributeWithNotNullTarget_then_fail() {
        createExtractors(null).extract(bond, null, null);
    }

    @Test
    public void when_creatingWithBuilder_then_evictableCacheIsUsed() {
        assertInstanceOf(EvictableGetterCache.class, Extractors.newBuilder(ss).build().getterCache);
    }

    @Test
    public void when_creatingWithBuilderWithSimpleGetterCache_then_simpleGetterCacheIsUsed() {
        Extractors extractors = Extractors.newBuilder(ss).setGetterCacheSupplier(SIMPLE_GETTER_CACHE_SUPPLIER).build();
        assertInstanceOf(SimpleGetterCache.class, extractors.getterCache);
    }

    private Extractors createExtractors(AttributeConfig config) {
        Extractors.Builder builder = Extractors.newBuilder(ss);
        if (config != null) {
            builder.setAttributeConfigs(singletonList(config));
        }
        if (useClassloader) {
            builder.setClassLoader(this.getClass().getClassLoader());
        }
        return builder.build();
    }

    private static class Bond {
        Car car = new Car();
    }

    private static class Car {
        int power = 550;
    }

    public static class PowerExtractor implements ValueExtractor<Bond, Object> {
        @Override
        public void extract(Bond target, Object arguments, ValueCollector collector) {
            collector.addObject(target.car.power);
        }
    }
}
