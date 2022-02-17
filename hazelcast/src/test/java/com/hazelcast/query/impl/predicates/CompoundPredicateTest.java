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
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test all classes which implement CompoundStatement in package com.hazelcast.query.impl.predicates
 * for compliance with CompoundStatement contract.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompoundPredicateTest {

    @Parameter
    public Class<? extends CompoundPredicate> klass;

    @Parameters(name = "{0}")
    public static Collection<Class<? extends CompoundPredicate>> getCompoundPredicateImplementations()
            throws ClassNotFoundException {
        // locate all classes which implement CompoundPredicate and exercise them
        return REFLECTIONS.getSubTypesOf(CompoundPredicate.class);
    }

    @Test
    public void test_newInstance()
            throws IllegalAccessException, InstantiationException {
        // all CompoundStatement classes must provide a default constructor
        Object o = klass.newInstance();
        assertTrue(o instanceof CompoundPredicate);
    }

    @Test
    public void test_whenSetPredicatesOnNewInstance()
            throws IllegalAccessException, InstantiationException {

        CompoundPredicate o = klass.newInstance();
        Predicate truePredicate = Predicates.alwaysTrue();
        o.setPredicates(new Predicate[]{truePredicate});
        assertEquals(truePredicate, o.getPredicates()[0]);
    }

    @Test(expected = IllegalStateException.class)
    public void test_whenSetPredicatesOnExistingPredicates_thenThrowException()
            throws IllegalAccessException, InstantiationException {

        CompoundPredicate o = klass.newInstance();
        Predicate truePredicate = Predicates.alwaysTrue();
        o.setPredicates(new Predicate[]{truePredicate});

        Predicate falsePredicate = Predicates.alwaysFalse();
        o.setPredicates(new Predicate[]{falsePredicate});

        fail();
    }

}
