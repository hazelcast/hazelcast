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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.query.impl.predicates.PredicateTestUtils.createMockVisitablePredicate;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.createPassthroughVisitor;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NotPredicateTest {
    private SerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void negate_thenReturnInnerPredicate() {
        Predicate inner = mock(Predicate.class);
        NotPredicate notPredicate = new NotPredicate(inner);
        Predicate negate = notPredicate.negate();

        assertThat(negate, sameInstance(inner));
    }

    @Test
    public void apply() {
        apply(Predicates.alwaysTrue(), false);
        apply(Predicates.alwaysFalse(), true);
    }

    public void apply(Predicate inner, boolean expected) {
        NotPredicate notPredicate = new NotPredicate(inner);

        boolean result = notPredicate.apply(mock(Map.Entry.class));

        assertEquals(expected, result);
    }

    @Test
    public void serialize() {
        NotPredicate notPredicate = new NotPredicate(Predicates.alwaysTrue());

        Data data = serializationService.toData(notPredicate);
        Object result = serializationService.toObject(data);

        NotPredicate found = assertInstanceOf(NotPredicate.class, result);
        assertInstanceOf(TruePredicate.class, found.predicate);
    }

    @Test
    public void accept_whenNullPredicate_thenReturnItself() {
        Visitor mockVisitor = createPassthroughVisitor();
        Indexes mockIndexes = mock(Indexes.class);

        NotPredicate notPredicate = new NotPredicate(null);
        NotPredicate result = (NotPredicate) notPredicate.accept(mockVisitor, mockIndexes);

        assertThat(result, sameInstance(notPredicate));
    }

    @Test
    public void accept_whenPredicateChangedOnAccept_thenReturnAndNewNotPredicate() {
        Visitor mockVisitor = createPassthroughVisitor();
        Indexes mockIndexes = mock(Indexes.class);

        Predicate transformed = mock(Predicate.class);
        Predicate predicate = createMockVisitablePredicate(transformed);

        NotPredicate notPredicate = new NotPredicate(predicate);
        NotPredicate result = (NotPredicate) notPredicate.accept(mockVisitor, mockIndexes);

        assertThat(result, not(sameInstance(notPredicate)));
        assertThat(result.predicate, equalTo(transformed));
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(NotPredicate.class)
            .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
            .verify();
    }

}
