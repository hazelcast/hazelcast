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

package com.hazelcast.map;

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.query.impl.predicates.RuleBasedQueryOptimizer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PagingPredicateOptimizationTest extends HazelcastTestSupport {

    @Test
    public void testInnerPredicateOptimization() {
        RuleBasedQueryOptimizer optimizer = new RuleBasedQueryOptimizer();
        Indexes indexes = mock(Indexes.class);

        Predicate[] orPredicates = new Predicate[10];
        for (int i = 0; i < orPredicates.length; ++i) {
            orPredicates[i] = Predicates.equal("a", i);
        }
        Predicate innerPredicate = Predicates.or(orPredicates);
        PagingPredicate<Object, Object> pagingPredicate = Predicates.pagingPredicate(innerPredicate, 10);

        Predicate optimized = optimizer.optimize(pagingPredicate, indexes);
        assertInstanceOf(PagingPredicateImpl.class, optimized);
        Predicate innerOptimized = ((PagingPredicateImpl) optimized).getPredicate();
        assertInstanceOf(InPredicate.class, innerOptimized);
    }

}
