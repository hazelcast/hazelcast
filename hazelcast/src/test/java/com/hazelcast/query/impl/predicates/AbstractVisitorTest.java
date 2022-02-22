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

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractVisitorTest {

    @Test
    public void testAllVisitMethodReturnTheOriginalPredicate() throws Exception {
        // Contract of AbstractVisitor mandates to return original predicate
        // for all methods on Visitor interface.

        // This test makes sure if a new method is added into Visitor interface
        // then it's added to AbstractVisitor and honour its contract
        AbstractVisitor visitor = new AbstractVisitor() {
        };
        Method[] methods = Visitor.class.getMethods();
        for (Method method : methods) {
            Class<?> predicateType = method.getParameterTypes()[0];
            Predicate predicate = (Predicate) predicateType.newInstance();
            Indexes indexes = mock(Indexes.class);
            Object result = method.invoke(visitor, predicate, indexes);

            assertSame("Method " + method + " does not return identity of the original predicate."
                    + " See contract of " + AbstractVisitor.class.getSimpleName(), predicate, result);
        }
    }
}
