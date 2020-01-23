/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LikePredicateTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testILikePredicateUnicodeCase() {
        assertTrue(new ILikePredicate("this", "Hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new ILikePredicate("this", "hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new ILikePredicate("this", "Хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
        assertTrue(new ILikePredicate("this", "хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateUnicodeCase() {
        assertTrue(new LikePredicate("this", "Hazelcast%").apply(entry("Hazelcast is here!")));
        assertFalse(new LikePredicate("this", "hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "Хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
        assertFalse(new LikePredicate("this", "хазелкаст%").apply(entry("Хазелкаст с большой буквы")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateSyntax() {
        assertTrue(new LikePredicate("this", "%Hazelcast%").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "%here_").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "%%").apply(entry("Hazelcast is here!")));
        assertTrue(new LikePredicate("this", "%%").apply(entry("")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLikePredicateSyntaxEscape() {
        assertTrue(new LikePredicate("this", "%\\_is\\_%").apply(entry("Hazelcast_is_here!")));
        assertTrue(new LikePredicate("this", "%is\\%here!").apply(entry("Hazelcast%is%here!")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void negative_testLikePredicateSyntax() {
        assertFalse(new LikePredicate("this", "_Hazelcast%").apply(entry("Hazelcast is here!")));
        assertFalse(new LikePredicate("this", "_").apply(entry("")));
        assertFalse(new LikePredicate("this", "Hazelcast%").apply(entry("")));
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(LikePredicate.class)
            .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
            .withRedefinedSuperclass()
            .allFieldsShouldBeUsed()
            .verify();
    }

}
