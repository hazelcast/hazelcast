/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings({"EqualsWithItself", "java:S5863"})
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeValueTest {

    @Test
    public void testConstruction() {
        assertThat(new CompositeValue(new Comparable[]{1, 1.1, "1.1"}).getComponents()).isEqualTo(new Object[]{1, 1.1, "1.1"});
        assertThat(new CompositeValue(1, "prefix", null).getComponents())
                .isEqualTo(new Object[]{"prefix"});
        assertThat(new CompositeValue(2, "prefix", "filler").getComponents())
                .isEqualTo(new Object[]{"prefix", "filler"});
        assertThat(new CompositeValue(5, "prefix", "filler").getComponents())
                .isEqualTo(new Object[]{"prefix", "filler", "filler", "filler", "filler"});
    }

    @Test
    public void testEqualsAndHashCode() {
        CompositeValue a = value("a", 'a');
        assertThat(a).isNotNull();
        assertThat(a).isEqualTo(a);

        assertThat(value("a", 'a')).isEqualTo(value("a", 'a'));
        assertThat(value("a", 'a').hashCode()).isEqualTo(value("a", 'a').hashCode());

        assertThat(value("a", 'a')).isNotEqualTo(value('a', "a"));
        assertThat(value("a", 'a')).isNotEqualTo(value("b", 'b'));
        assertThat(value("a", 'a')).isNotEqualTo(value('b', "b"));
        assertThat(value("a", 'a')).isNotEqualTo(value(null, 'a'));
        assertThat(value("a", 'a')).isNotEqualTo(value("a", NULL));
        assertThat(value("a", 'a')).isNotEqualTo(value("a", NEGATIVE_INFINITY));
        assertThat(value("a", 'a')).isNotEqualTo(value("a", POSITIVE_INFINITY));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSpecialValuesOrdering() {
        assertThat(NULL).isEqualByComparingTo(NULL);
        assertThat(NULL).isEqualTo(NULL);
        assertThat(NULL).isGreaterThan(NEGATIVE_INFINITY);
        assertThat(NULL).isLessThan(POSITIVE_INFINITY);

        assertThat(NEGATIVE_INFINITY).isLessThan(NULL);
        assertThat(NEGATIVE_INFINITY).isLessThan(POSITIVE_INFINITY);

        assertThat(POSITIVE_INFINITY).isGreaterThan(NULL);
        assertThat(POSITIVE_INFINITY).isGreaterThan(NEGATIVE_INFINITY);

        assertThat((Comparable) NULL).isLessThan("a");
        assertThat((Comparable) NEGATIVE_INFINITY).isLessThan("a");
        assertThat((Comparable) POSITIVE_INFINITY).isGreaterThan("a");
    }

    @Test
    public void testComparable() {
        CompositeValue a = value("a", 'a');
        assertThat(a).isEqualByComparingTo(a);

        assertThat(value("a", 'a')).isEqualByComparingTo(value("a", 'a'));
        assertThat(value("a", 'a')).isNotEqualByComparingTo(value("b", 'b'));
        assertThat(value("a", 'a')).isNotEqualByComparingTo(value("a", NULL));
        assertThat(value("a", 'a')).isNotEqualByComparingTo(value("a", NEGATIVE_INFINITY));
        assertThat(value("a", 'a')).isNotEqualByComparingTo(value("a", POSITIVE_INFINITY));

        assertThat(value("b", 'b')).isLessThan(value("b", 'c'));
        assertThat(value("b", 'b')).isGreaterThan(value("b", 'a'));
        assertThat(value("b", 'b')).isLessThan(value("c", 'b'));
        assertThat(value("b", 'b')).isGreaterThan(value("a", 'b'));
        assertThat(value("b", 'b')).isLessThan(value("c", 'c'));
        assertThat(value("b", 'b')).isGreaterThan(value("a", 'a'));

        assertThat(value("b", 'b')).isGreaterThan(value(NULL, NULL));
        assertThat(value("b", NULL)).isGreaterThan(value(NULL, NULL));
        assertThat(value(NULL, 'b')).isGreaterThan(value(NULL, NULL));
        assertThat(value(NULL, NULL)).isEqualByComparingTo(value(NULL, NULL));
        assertThat(value("b", 'b')).isGreaterThan(value("b", NULL));
        assertThat(value("b", 'b')).isGreaterThan(value(NULL, 'b'));

        assertThat(value("b", 'b')).isGreaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value("b", NULL)).isGreaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value(NULL, 'b')).isGreaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value(NULL, NULL)).isGreaterThan(value(NEGATIVE_INFINITY, NEGATIVE_INFINITY));

        assertThat(value("b", 'b')).isLessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value("b", NULL)).isLessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value(NULL, 'b')).isLessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value(NULL, NULL)).isLessThan(value(POSITIVE_INFINITY, NEGATIVE_INFINITY));

        assertThat(value("b", 'b')).isGreaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY));
        assertThat(value("b", NULL)).isGreaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY));
        assertThat(value(NULL, 'b')).isGreaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY));
        assertThat(value(NULL, NULL)).isGreaterThan(value(NEGATIVE_INFINITY, POSITIVE_INFINITY));

        assertThat(value("b", 'b')).isLessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY));
        assertThat(value("b", NULL)).isLessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY));
        assertThat(value(NULL, 'b')).isLessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY));
        assertThat(value(NULL, NULL)).isLessThan(value(POSITIVE_INFINITY, POSITIVE_INFINITY));

        assertThat(value("b", 'b')).isGreaterThan(value("b", NEGATIVE_INFINITY));
        assertThat(value("b", 'b')).isLessThan(value("b", POSITIVE_INFINITY));
        assertThat(value("b", 'b')).isGreaterThan(value("a", NEGATIVE_INFINITY));
        assertThat(value("b", 'b')).isGreaterThan(value("a", POSITIVE_INFINITY));
        assertThat(value("b", 'b')).isLessThan(value("c", NEGATIVE_INFINITY));
        assertThat(value("b", 'b')).isLessThan(value("c", POSITIVE_INFINITY));
    }

    @Test
    public void testSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data data = serializationService.toData(value(1.0, "a", NULL, POSITIVE_INFINITY, NEGATIVE_INFINITY));
        assertThat(value(1.0, "a", NULL, POSITIVE_INFINITY, NEGATIVE_INFINITY)).isEqualTo(serializationService.toObject(data));
    }

    private static CompositeValue value(Comparable<?>... components) {
        return new CompositeValue(components);
    }

}
