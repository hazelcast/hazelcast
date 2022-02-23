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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MeasurementPredicatesTest {

    private Measurement measurement;

    @Before
    public void before() {
        measurement = Measurement.of(
                "metric",
                13L,
                System.currentTimeMillis(),
                Stream.of(
                        entry("tag1", "valA"),
                        entry("tag2", "valB"),
                        entry("tag3", "valA")
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }

    @Test
    public void containsTag() {
        assertTrue(MeasurementPredicates.containsTag("tag2").test(measurement));
        assertFalse(MeasurementPredicates.containsTag("tagX").test(measurement));
    }

    @Test
    public void tagValueEquals() {
        assertTrue(MeasurementPredicates.tagValueEquals("tag2", "valB").test(measurement));
        assertFalse(MeasurementPredicates.tagValueEquals("tag2", "valX").test(measurement));
    }

    @Test
    public void tagValueMatches() {
        assertTrue(MeasurementPredicates.tagValueMatches("tag2", "val[A-Z]+").test(measurement));
        assertFalse(MeasurementPredicates.tagValueMatches("tag2", "val[0-9]+").test(measurement));
    }

}
