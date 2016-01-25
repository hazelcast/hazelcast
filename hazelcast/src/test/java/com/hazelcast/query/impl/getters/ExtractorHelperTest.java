package com.hazelcast.query.impl.getters;

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.query.impl.getters.ExtractorHelper.extractArgumentsFromAttributeName;
import static com.hazelcast.query.impl.getters.ExtractorHelper.extractAttributeNameNameWithoutArguments;
import static groovy.util.GroovyTestCase.assertEquals;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtractorHelperTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void instantiate_extractor() {
        // GIVEN
        MapAttributeConfig config = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");

        // WHEN
        ValueExtractor extractor = ExtractorHelper.instantiateExtractor(config);

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
        ExtractorHelper.instantiateExtractor(config);
    }

    @Test
    public void instantiate_extractors() {
        // GIVEN
        MapAttributeConfig iqExtractor = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        MapAttributeConfig nameExtractor = new MapAttributeConfig("name", "com.hazelcast.query.impl.getters.ExtractorHelperTest$NameExtractor");

        // WHEN
        Map<String, ValueExtractor> extractors = ExtractorHelper.instantiateExtractors(asList(iqExtractor, nameExtractor));

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
        ExtractorHelper.instantiateExtractors(asList(iqExtractor, nameExtractor));
    }

    @Test
    public void instantiate_extractors_duplicateExtractor() {
        // GIVEN
        MapAttributeConfig iqExtractor = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");
        MapAttributeConfig iqExtractorDuplicate = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$IqExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        ExtractorHelper.instantiateExtractors(asList(iqExtractor, iqExtractorDuplicate));
    }

    @Test
    public void instantiate_extractors_wrongType() {
        // GIVEN
        MapAttributeConfig string = new MapAttributeConfig("iq", "java.lang.String");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        ExtractorHelper.instantiateExtractors(asList(string));
    }

    @Test
    public void instantiate_extractors_initException() {
        // GIVEN
        MapAttributeConfig string = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$InitExceptionExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        ExtractorHelper.instantiateExtractors(asList(string));
    }

    @Test
    public void instantiate_extractors_accessException() {
        // GIVEN
        MapAttributeConfig string = new MapAttributeConfig("iq", "com.hazelcast.query.impl.getters.ExtractorHelperTest$AccessExceptionExtractor");

        // EXPECT
        expected.expect(IllegalArgumentException.class);

        // WHEN
        ExtractorHelper.instantiateExtractors(asList(string));
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

    @Test(expected = IllegalArgumentException.class)
    public void extractArgument_wrongArguments_tooManySquareBrackets() {
        extractArgumentsFromAttributeName("car.wheel[2].pressure[BAR]");
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

    @Test(expected = IllegalArgumentException.class)
    public void extractAttributeName_wrongArguments_tooManySquareBrackets() {
        extractArgumentsFromAttributeName("car.wheel[2].pressure[BAR]");
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

}
