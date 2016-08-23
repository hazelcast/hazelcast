package com.hazelcast.query.impl.getters;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;
import static groovy.util.GroovyTestCase.assertEquals;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class ExtractorHelperTest {

    @Parameterized.Parameters(name = "useClassloader:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false},
                {true}
        });
    }

    @Parameterized.Parameter(0)
    public boolean useClassloader;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void instantiate_extractor() {
        // GIVEN
        MapAttributeConfig config = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");

        // WHEN
        ValueExtractor extractor = instantiateExtractor(config);

        // THEN
        assertThat(extractor, instanceOf(IqExtractor.class));
    }

    @Test
    public void instantiate_extractor_notExistingClass() {
        // GIVEN
        MapAttributeConfig config = new MapAttributeConfig("iq", "not.existing.class");

        // EXPECT
        expected.expect(IllegalArgumentException.class);
        expected.expectCause(isA(ClassNotFoundException.class));

        // WHEN
        instantiateExtractor(config);
    }

    @Test
    public void instantiate_extractors() {
        // GIVEN
        MapAttributeConfig iqExtractor = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        MapAttributeConfig nameExtractor = new MapAttributeConfig("name", "com.hazelcast.query.impl.getters.ExtractorHelperTest$NameExtractor");

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
        MapAttributeConfig iqExtractor =
                new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        MapAttributeConfig nameExtractor =
                new MapAttributeConfig("name", "com.hazelcast.query.impl.getters.ExtractorHelperTest$NameExtractor");
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
        MapAttributeConfig iqExtractor = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        MapAttributeConfig nameExtractor = new MapAttributeConfig("name", "not.existing.class");

        // EXPECT
        expected.expect(IllegalArgumentException.class);
        expected.expectCause(isA(ClassNotFoundException.class));

        // WHEN
        instantiateExtractors(asList(iqExtractor, nameExtractor));
    }

    @Test
    public void instantiate_extractors_duplicateExtractor() {
        // GIVEN
        MapAttributeConfig iqExtractor = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        MapAttributeConfig iqExtractorDuplicate = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(asList(iqExtractor, iqExtractorDuplicate));
    }

    @Test
    public void instantiate_extractors_wrongType() {
        // GIVEN
        MapAttributeConfig string = new MapAttributeConfig("iq", "java.lang.String");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(asList(string));
    }

    @Test
    public void instantiate_extractors_initException() {
        // GIVEN
        MapAttributeConfig string = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$InitExceptionExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(asList(string));
    }

    @Test
    public void instantiate_extractors_accessException() {
        // GIVEN
        MapAttributeConfig string = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$AccessExceptionExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        instantiateExtractors(asList(string));
    }

    @Test
    public void extractArgument_correctArguments() {
        assertEquals("left-front", extractArgumentsFromAttributeName("car.wheel[left-front]"));
        assertEquals("123", extractArgumentsFromAttributeName("car.wheel[123]"));
        assertEquals(".';'.", extractArgumentsFromAttributeName("car.wheel[.';'.]"));
        assertEquals("", extractArgumentsFromAttributeName("car.wheel[]"));
        assertEquals(null, extractArgumentsFromAttributeName("car.wheel"));
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

    public static class IqExtractor extends ValueExtractor<Object, Object> {
        @Override
        public void extract(Object target, Object arguments, ValueCollector collector) {
        }
    }

    public static class AccessExceptionExtractor extends NameExtractor {
        private AccessExceptionExtractor() {
        }
    }

    public static abstract class InitExceptionExtractor extends NameExtractor {
    }

    public static class NameExtractor extends ValueExtractor<Object, Object> {
        @Override
        public void extract(Object target, Object arguments, ValueCollector collector) {
        }
    }

    private ValueExtractor instantiateExtractor(MapAttributeConfig mapAttributeConfig) {
        return ExtractorHelper.instantiateExtractor(mapAttributeConfig,
                useClassloader ? this.getClass().getClassLoader() : null, Logger.noLogger());
    }

    private Map<String, ValueExtractor> instantiateExtractors(List<MapAttributeConfig> mapAttributeConfigs) {
        return ExtractorHelper.instantiateExtractors(mapAttributeConfigs,
                useClassloader ? this.getClass().getClassLoader() : null, Logger.noLogger());
    }

}
