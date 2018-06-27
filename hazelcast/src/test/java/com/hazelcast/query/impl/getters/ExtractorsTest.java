/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
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

    private InternalSerializationService UNUSED;

    @Test
    public void getGetter_reflection_cachingWorks() {
        // GIVEN
        Extractors extractors = extractors();

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(UNUSED, bond, "car.power");
        Getter getterSecondInvocation = extractors.getGetter(UNUSED, bond, "car.power");

        // THEN
        assertThat(getterFirstInvocation, sameInstance(getterSecondInvocation));
        assertThat(getterFirstInvocation, instanceOf(FieldGetter.class));
    }

    @Test
    public void extract_reflection_correctValue() {
        // WHEN
        Object power = extractors().extract(UNUSED, bond, "car.power");

        // THEN
        assertThat((Integer) power, equalTo(550));
    }

    @Test
    public void getGetter_extractor_cachingWorks() {
        // GIVEN
        MapAttributeConfig config
                = new MapAttributeConfig("gimmePower", "com.hazelcast.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = new Extractors(singletonList(config), useClassloader ? this.getClass().getClassLoader() : null);

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(UNUSED, bond, "gimmePower");
        Getter getterSecondInvocation = extractors.getGetter(UNUSED, bond, "gimmePower");

        // THEN
        assertThat(getterFirstInvocation, sameInstance(getterSecondInvocation));
        assertThat(getterFirstInvocation, instanceOf(ExtractorGetter.class));
    }

    @Test
    public void extract_extractor_correctValue() {
        // GIVEN
        MapAttributeConfig config
                = new MapAttributeConfig("gimmePower", "com.hazelcast.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = new Extractors(singletonList(config), useClassloader ? this.getClass().getClassLoader() : null);

        // WHEN
        Object power = extractors.extract(UNUSED, bond, "gimmePower");

        // THEN
        assertThat((Integer) power, equalTo(550));
    }

    @Test
    public void extract_nullTarget() {
        // WHEN
        Object power = extractors().extract(UNUSED, null, "gimmePower");

        // THEN
        assertNull(power);
    }

    @Test
    public void extract_nullAll() {
        // WHEN
        Object power = extractors().extract(UNUSED, null, null);

        // THEN
        assertNull(power);
    }

    @Test(expected = NullPointerException.class)
    public void extract_nullAttribute() {
        extractors().extract(UNUSED, bond, null);
    }

    private static class Bond {
        Car car = new Car();
    }

    private static class Car {
        int power = 550;
    }

    public static class PowerExtractor extends ValueExtractor<Bond, Object> {
        @Override
        public void extract(Bond target, Object arguments, ValueCollector collector) {
            collector.addObject(target.car.power);
        }
    }

    private static Extractors extractors() {
        return new Extractors(Collections.<MapAttributeConfig>emptyList(), null);
    }
}
