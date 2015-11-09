package com.hazelcast.query.impl.getters;

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtractorsTest {

    private Bond bond = new Bond();

    @Test
    public void getGetter_reflection_cachingWorks() {
        // GIVEN
        Extractors extractors = extractors();

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(bond, "car.power");
        Getter getterSecondInvocation = extractors.getGetter(bond, "car.power");

        // THEN
        assertThat(getterFirstInvocation, sameInstance(getterSecondInvocation));
        assertThat(getterFirstInvocation, instanceOf(FieldGetter.class));
    }

    @Test
    public void extract_reflection_correctValue() {
        // WHEN
        Object power = extractors().extract(bond, "car.power");

        // THEN
        assertThat((Integer) power, equalTo(550));
    }

    @Test
    public void getGetter_extractor_cachingWorks() {
        // GIVEN
        MapAttributeConfig config = new MapAttributeConfig("gimmePower", "com.hazelcast.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = new Extractors(asList(config));

        // WHEN
        Getter getterFirstInvocation = extractors.getGetter(bond, "gimmePower");
        Getter getterSecondInvocation = extractors.getGetter(bond, "gimmePower");

        // THEN
        assertThat(getterFirstInvocation, sameInstance(getterSecondInvocation));
        assertThat(getterFirstInvocation, instanceOf(ExtractorGetter.class));
    }

    @Test
    public void extract_extractor_correctValue() {
        // GIVEN
        MapAttributeConfig config = new MapAttributeConfig("gimmePower", "com.hazelcast.query.impl.getters.ExtractorsTest$PowerExtractor");
        Extractors extractors = new Extractors(asList(config));

        // WHEN
        Object power = extractors.extract(bond, "gimmePower");

        // THEN
        assertThat((Integer) power, equalTo(550));
    }

    @Test
    public void extract_nullTarget() {
        // WHEN
        Object power = extractors().extract(null, "gimmePower");

        // THEN
        assertNull(power);
    }

    @Test
    public void extract_nullAll() {
        // WHEN
        Object power = extractors().extract(null, null);

        // THEN
        assertNull(power);
    }

    @Test(expected = NullPointerException.class)
    public void extract_nullAttribute() {
        extractors().extract(bond, null);
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
        return new Extractors(Collections.<MapAttributeConfig>emptyList());
    }

}
