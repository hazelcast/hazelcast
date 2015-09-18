/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GreaterLessPredicateTest {

    @Test
    public void negate_whenEqualsTrueAndLessTrue_thenReturnNewInstanceWithEqualsFalseAndLessFalse() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, true, true);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate, not(sameInstance(original)));
        assertThat(negate.attribute, equalTo(attribute));
        assertThat(negate.equal, is(false));
        assertThat(negate.less, is(false));
    }

    @Test
    public void negate_whenEqualsFalseAndLessFalse_thenReturnNewInstanceWithEqualsTrueAndLessTrue() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, false, false);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate, not(sameInstance(original)));
        assertThat(negate.attribute, equalTo(attribute));
        assertThat(negate.equal, is(true));
        assertThat(negate.less, is(true));
    }

    @Test
    public void negate_whenEqualsTrueAndLessFalse_thenReturnNewInstanceWithEqualsFalseAndLessTrue() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, true, false);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate, not(sameInstance(original)));
        assertThat(negate.attribute, equalTo(attribute));
        assertThat(negate.equal, is(false));
        assertThat(negate.less, is(true));
    }

    @Test
    public void negate_whenEqualsFalseAndLessTrue_thenReturnNewInstanceWithEqualsTrueAndLessFalse() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, false, true);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate, not(sameInstance(original)));
        assertThat(negate.attribute, equalTo(attribute));
        assertThat(negate.equal, is(true));
        assertThat(negate.less, is(false));
    }
}
