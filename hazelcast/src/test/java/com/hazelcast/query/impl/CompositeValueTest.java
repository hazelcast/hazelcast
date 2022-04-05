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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeValueTest {

    @Test
    public void testConstruction() {
        assertThat(new CompositeValue(new Comparable[]{1, 1.1, "1.1"}).getComponents(), equalTo(new Object[]{1, 1.1, "1.1"}));
        assertThat(new CompositeValue(1, "prefix", null).getComponents(), equalTo(new Object[]{"prefix"}));
        assertThat(new CompositeValue(2, "prefix", "filler").getComponents(), equalTo(new Object[]{"prefix", "filler"}));
        assertThat(new CompositeValue(5, "prefix", "filler").getComponents(),
                equalTo(new Object[]{"prefix", "filler", "filler", "filler", "filler"}));
    }

    @Test
    public void testEqualsAndHashCode() {
        CompositeValue a = value("a", 'a');
        assertThat(a, equalTo(a));
        assertThat(a, not(equalTo(null)));

        assertThat(value("a", 'a'), equalTo(value("a", 'a')));
        assertThat(value("a", 'a').hashCode(), equalTo(value("a", 'a').hashCode()));

        assertThat(value("a", 'a'), not(equalTo(value('a', "a"))));
        assertThat(value("a", 'a'), not(equalTo(value("b", 'b'))));
        assertThat(value("a", 'a'), not(equalTo(value('b', "b"))));
        assertThat(value("a", 'a'), not(equalTo(value(null, 'a'))));
        assertThat(value("a", 'a'), not(equalTo(value("a", NULL))));
        assertThat(value("a", 'a'), not(equalTo(value("a", NEGATIVE_INFINITY))));
        assertThat(value("a", 'a'), not(equalTo(value("a", POSITIVE_INFINITY))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSpecialValuesOrdering() {
        MatcherAssert.<ComparableIdentifiedDataSerializable>assertThat(NULL, comparesEqualTo(NULL));
        assertThat(NULL, equalTo(NULL));
        MatcherAssert.<ComparableIdentifiedDataSerializable>assertThat(NULL, greaterThan(NEGATIVE_INFINITY));
        MatcherAssert.<ComparableIdentifiedDataSerializable>assertThat(NULL, lessThan(POSITIVE_INFINITY));

        MatcherAssert.<ComparableIdentifiedDataSerializable>assertThat(NEGATIVE_INFINITY, lessThan(NULL));
        MatcherAssert.<ComparableIdentifiedDataSerializable>assertThat(NEGATIVE_INFINITY, lessThan(POSITIVE_INFINITY));

        MatcherAssert.<ComparableIdentifiedDataSerializable>assertThat(POSITIVE_INFINITY, greaterThan(NULL));
        MatcherAssert.<ComparableIdentifiedDataSerializable>assertThat(POSITIVE_INFINITY, greaterThan(NEGATIVE_INFINITY));

        MatcherAssert.<Comparable>assertThat(NULL, lessThan((Comparable) "a"));
        MatcherAssert.<Comparable>assertThat(NEGATIVE_INFINITY, lessThan((Comparable) "a"));
        MatcherAssert.<Comparable>assertThat(POSITIVE_INFINITY, greaterThan((Comparable) "a"));
    }

    @Test
    public void testComparable() {
        CompositeValue a = value("a", 'a');
        assertThat(a, comparesEqualTo(a));

        assertThat(value("a", 'a'), comparesEqualTo(value("a", 'a')));
        assertThat(value("a", 'a'), not(comparesEqualTo(value("b", 'b'))));
        assertThat(value("a", 'a'), not(comparesEqualTo(value("a", NULL))));
        assertThat(value("a", 'a'), not(comparesEqualTo(value("a", NEGATIVE_INFINITY))));
        assertThat(value("a", 'a'), not(comparesEqualTo(value("a", POSITIVE_INFINITY))));

        assertThat(value("b", 'b'), lessThan(value("b", 'c')));
        assertThat(value("b", 'b'), greaterThan(value("b", 'a')));
        assertThat(value("b", 'b'), lessThan(value("c", 'b')));
        assertThat(value("b", 'b'), greaterThan(value("a", 'b')));
        assertThat(value("b", 'b'), lessThan(value("c", 'c')));
        assertThat(value("b", 'b'), greaterThan(value("a", 'a')));

        assertThat(value("b", 'b'), greaterThan(value(NULL, NULL)));
        assertThat(value("b", NULL), greaterThan(value(NULL, NULL)));
        assertThat(value(NULL, 'b'), greaterThan(value(NULL, NULL)));
        assertThat(value(NULL, NULL), comparesEqualTo(value(NULL, NULL)));
        assertThat(value("b", 'b'), greaterThan(value("b", NULL)));
        assertThat(value("b", 'b'), greaterThan(value(NULL, 'b')));

        assertThat(value("b", 'b'), greaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY)));
        assertThat(value("b", NULL), greaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY)));
        assertThat(value(NULL, 'b'), greaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY)));
        assertThat(value(NULL, NULL), greaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY)));

        assertThat(value("b", 'b'), lessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY)));
        assertThat(value("b", NULL), lessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY)));
        assertThat(value(NULL, 'b'), lessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY)));
        assertThat(value(NULL, NULL), lessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY)));

        assertThat(value("b", 'b'), greaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY)));
        assertThat(value("b", NULL), greaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY)));
        assertThat(value(NULL, 'b'), greaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY)));
        assertThat(value(NULL, NULL), greaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY)));

        assertThat(value("b", 'b'), lessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY)));
        assertThat(value("b", NULL), lessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY)));
        assertThat(value(NULL, 'b'), lessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY)));
        assertThat(value(NULL, NULL), lessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY)));

        assertThat(value("b", 'b'), greaterThan(value("b", NEGATIVE_INFINITY)));
        assertThat(value("b", 'b'), lessThan(value("b", POSITIVE_INFINITY)));
        assertThat(value("b", 'b'), greaterThan(value("a", NEGATIVE_INFINITY)));
        assertThat(value("b", 'b'), greaterThan(value("a", POSITIVE_INFINITY)));
        assertThat(value("b", 'b'), lessThan(value("c", NEGATIVE_INFINITY)));
        assertThat(value("b", 'b'), lessThan(value("c", POSITIVE_INFINITY)));
    }

    @Test
    public void testSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data data = serializationService.toData(value(1.0, "a", NULL, POSITIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value(1.0, "a", NULL, POSITIVE_INFINITY, NEGATIVE_INFINITY), equalTo(serializationService.toObject(data)));
    }

    private static CompositeValue value(Comparable... components) {
        return new CompositeValue(components);
    }

}
