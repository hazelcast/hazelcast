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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static org.mockito.Mockito.mock;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OrToInVisitorTest {

    private OrToInVisitor visitor;
    private IndexRegistry indexes;

    @Before
    public void setUp() {
        indexes = mock(IndexRegistry.class);
        visitor = new OrToInVisitor();
    }

    @Test
    public void whenEmptyPredicate_thenReturnItself() {
        OrPredicate or = new OrPredicate((Predicate[]) null);
        OrPredicate result = (OrPredicate) visitor.visit(or, indexes);
        assertThat(or).isEqualTo(result);
    }

    @Test
    public void whenThresholdNotExceeded_thenReturnItself() {
        Predicate p1 = equal("age", 1);
        Predicate p2 = equal("age", 2);
        OrPredicate or = (OrPredicate) or(p1, p2);
        OrPredicate result = (OrPredicate) visitor.visit(or, indexes);
        assertThat(or).isEqualTo(result);
    }

    @Test
    public void whenThresholdExceeded_noCandidatesFound_thenReturnItself() {
        // (age != 1 or age != 2 or age != 3 or age != 4 or age != 5)
        Predicate p1 = notEqual("age", 1);
        Predicate p2 = notEqual("age", 2);
        Predicate p3 = notEqual("age", 3);
        Predicate p4 = notEqual("age", 4);
        Predicate p5 = notEqual("age", 5);
        OrPredicate or = (OrPredicate) or(p1, p2, p3, p4, p5);
        OrPredicate result = (OrPredicate) visitor.visit(or, indexes);
        assertThat(or).isEqualTo(result);
    }

    @Test
    public void whenThresholdExceeded_noEnoughCandidatesFound_thenReturnItself() {
        // (age != 1 or age != 2 or age != 3 or age != 4 or age != 5)
        Predicate p1 = equal("age", 1);
        Predicate p2 = equal("age", 2);
        Predicate p3 = equal("age", 3);
        Predicate p4 = equal("age", 4);
        Predicate p5 = notEqual("age", 5);
        OrPredicate or = (OrPredicate) or(p1, p2, p3, p4, p5);
        OrPredicate result = (OrPredicate) visitor.visit(or, indexes);
        assertThat(or).isEqualTo(result);
    }

    @Test
    public void whenThresholdExceeded_thenRewriteToInPredicate() {
        // (age = 1 or age = 2 or age = 3 or age = 4 or age = 5)  -->  (age in (1, 2, 3, 4, 5))
        Predicate p1 = equal("age", 1);
        Predicate p2 = equal("age", 2);
        Predicate p3 = equal("age", 3);
        Predicate p4 = equal("age", 4);
        Predicate p5 = equal("age", 5);
        OrPredicate or = (OrPredicate) or(p1, p2, p3, p4, p5);
        InPredicate result = (InPredicate) visitor.visit(or, indexes);
        Comparable[] values = result.values;
        assertThat(values).hasSize(5);
        assertThat(values).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    public void whenThresholdExceeded_thenRewriteToOrPredicate() {
        // (age = 1 or age = 2 or age = 3 or age = 4 or age = 5 or age != 6)  -->  (age in (1, 2, 3, 4, 5) or age != 6)
        Predicate p1 = equal("age", 1);
        Predicate p2 = equal("age", 2);
        Predicate p3 = equal("age", 3);
        Predicate p4 = equal("age", 4);
        Predicate p5 = equal("age", 5);
        Predicate p6 = notEqual("age", 6);
        OrPredicate or = (OrPredicate) or(p1, p2, p3, p4, p5, p6);
        OrPredicate result = (OrPredicate) visitor.visit(or, indexes);
        Predicate[] predicates = result.predicates;
        for (Predicate predicate : predicates) {
            if (predicate instanceof InPredicate) {
                Comparable[] values = ((InPredicate) predicate).values;
                assertThat(values).hasSize(5);
                assertThat(values).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
            } else {
                assertThat(predicate).isInstanceOf(NotEqualPredicate.class);
            }
        }
    }

}
