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
import com.hazelcast.config.Config;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;
import static groovy.util.GroovyTestCase.assertEquals;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class ExtractorHelperTest {

    @Parameters(name = "useClassloader:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Parameter
    public boolean useClassloader;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void instantiate_extractor() {
        // GIVEN
        AttributeConfig config
                = new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");

        // WHEN
        ValueExtractor extractor = instantiateExtractor(config);

        // THEN
        assertThat(extractor, instanceOf(IqExtractor.class));
    }

    @Test
    public void instantiate_extractor_notExistingClass() {
        // GIVEN
        AttributeConfig config = new AttributeConfig("iq", "not.existing.class");

        // EXPECT
        expected.expect(IllegalArgumentException.class);
        expected.expectCause(isA(ClassNotFoundException.class));

        // WHEN
        instantiateExtractor(config);
    }

    @Test
    public void instantiate_extractors() {
        // GIVEN
        AttributeConfig iqExtractor
                = new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        AttributeConfig nameExtractor
                = new AttributeConfig("name", "com.hazelcast.query.impl.getters.ExtractorHelperTest$NameExtractor");

        // WHEN
        Map<String, ValueExtractor> extractors =
                instantiateExtractors(asList(iqExtractor, nameExtractor));

        // THEN
        assertThat(extractors.get("iq"), instanceOf(IqExtractor.class));
        assertThat(extractors.get("name"), instanceOf(NameExtractor.class));
    }

    @Test
    public void instantiate_extractors_withCustomClassLoader() {
        // GIVEN
        AttributeConfig iqExtractor =
                new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        AttributeConfig nameExtractor =
                new AttributeConfig("name", "com.hazelcast.query.impl.getters.ExtractorHelperTest$NameExtractor");
        Config config = new Config();
        // For other custom class loaders (from OSGi bundles, for example)
        ClassLoader customClassLoader = getClass().getClassLoader();
        config.setClassLoader(customClassLoader);

        // WHEN
        Map<String, ValueExtractor> extractors = instantiateExtractors(asList(iqExtractor, nameExtractor));

        // THEN
        assertThat(extractors.get("iq"), instanceOf(IqExtractor.class));
        assertThat(extractors.get("name"), instanceOf(NameExtractor.class));
    }

    @Test
    public void instantiate_extractors_oneClassNotExisting() {
        // GIVEN
        AttributeConfig iqExtractor
                = new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        AttributeConfig nameExtractor = new AttributeConfig("name", "not.existing.class");

        // EXPECT
        expected.expect(IllegalArgumentException.class);
        expected.expectCause(isA(ClassNotFoundException.class));

        // WHEN
        instantiateExtractors(asList(iqExtractor, nameExtractor));
    }

    @Test
    public void instantiate_extractors_duplicateExtractor() {
        // GIVEN
        AttributeConfig iqExtractor
                = new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        AttributeConfig iqExtractorDuplicate
                = new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(asList(iqExtractor, iqExtractorDuplicate));
    }

    @Test
    public void instantiate_extractors_wrongType() {
        // GIVEN
        AttributeConfig string = new AttributeConfig("iq", "java.lang.String");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(singletonList(string));
    }

    @Test
    public void instantiate_extractors_initException() {
        // GIVEN
        AttributeConfig string
                = new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$InitExceptionExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(singletonList(string));
    }

    @Test
    public void instantiate_extractors_accessException() {
        // GIVEN
        AttributeConfig string
                = new AttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$AccessExceptionExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(singletonList(string));
    }

    @Test
    public void extractArgument_correctArguments() {
        assertEquals("left-front", extractArgumentsFromAttributeName("car.wheel[left-front]"));
        assertEquals("123", extractArgumentsFromAttributeName("car.wheel[123]"));
        assertEquals(".';'.", extractArgumentsFromAttributeName("car.wheel[.';'.]"));
        assertEquals("", extractArgumentsFromAttributeName("car.wheel[]"));
        assertNull(extractArgumentsFromAttributeName("car.wheel"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractArgument_wrongArguments_noClosing() {
        extractArgumentsFromAttributeName("car.wheel[left");
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractArgument_wrongArguments_noArgument() {
        extractArgumentsFromAttributeName("car.wheel[");
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractArgument_wrongArguments_noOpening() {
        extractArgumentsFromAttributeName("car.wheelleft]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractArgument_wrongArguments_noArgument_noOpening() {
        extractArgumentsFromAttributeName("car.wheel]");
    }

    @Test
    public void extractArgument_wrongArguments_tooManySquareBrackets_lastExtracted() {
        assertEquals("BAR", extractArgumentsFromAttributeName("car.wheel[2].pressure[BAR]"));
    }

    @Test
    public void extractAttributeName_correctArguments() {
        assertEquals("car.wheel", extractAttributeNameNameWithoutArguments("car.wheel[left-front]"));
        assertEquals("car.wheel", extractAttributeNameNameWithoutArguments("car.wheel[123]"));
        assertEquals("car.wheel", extractAttributeNameNameWithoutArguments("car.wheel[.';'.]"));
        assertEquals("car.wheel", extractAttributeNameNameWithoutArguments("car.wheel[]"));
        assertEquals("car.wheel", extractAttributeNameNameWithoutArguments("car.wheel"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractAttributeName_wrongArguments_noClosing() {
        extractAttributeNameNameWithoutArguments("car.wheel[left");
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractAttributeName_wrongArguments_noArgument() {
        extractAttributeNameNameWithoutArguments("car.wheel[");
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractAttributeName_wrongArguments_noOpening() {
        extractAttributeNameNameWithoutArguments("car.wheelleft]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractAttributeName_wrongArguments_noArgument_noOpening() {
        extractAttributeNameNameWithoutArguments("car.wheel]");
    }

    @Test
    public void extractAttributeName_wrongArguments_tooManySquareBrackets_lastExtracted() {
        assertEquals("car.wheel[2].pressure", extractAttributeNameNameWithoutArguments("car.wheel[2].pressure[BAR]"));
    }

    private ValueExtractor instantiateExtractor(AttributeConfig attributeConfig) {
        return ExtractorHelper.instantiateExtractor(attributeConfig,
                useClassloader ? this.getClass().getClassLoader() : null);
    }

    private Map<String, ValueExtractor> instantiateExtractors(List<AttributeConfig> attributeConfigs) {
        return ExtractorHelper.instantiateExtractors(attributeConfigs,
                useClassloader ? this.getClass().getClassLoader() : null);
    }

    public abstract class InitExceptionExtractor extends NameExtractor {
    }

    public static final class IqExtractor implements ValueExtractor<Object, Object> {

        @Override
        public void extract(Object target, Object arguments, ValueCollector collector) {
        }
    }

    public static final class AccessExceptionExtractor extends NameExtractor {

        private AccessExceptionExtractor() {
        }
    }

    public static class NameExtractor implements ValueExtractor<Object, Object> {

        @Override
        public void extract(Object target, Object arguments, ValueCollector collector) {
        }
    }
}
