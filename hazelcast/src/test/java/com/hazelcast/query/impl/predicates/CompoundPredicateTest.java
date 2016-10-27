package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test all classes which implement CompoundStatement in package com.hazelcast.query.impl.predicates
 * for compliance with CompoundStatement contract.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CompoundPredicateTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Class<? extends CompoundPredicate>> getCompoundPredicateImplementations()
            throws ClassNotFoundException {
        // locate all classes which implement CompoundPredicate and exercise them
        return REFLECTIONS.getSubTypesOf(CompoundPredicate.class);
    }

    @Parameterized.Parameter
    public Class<? extends CompoundPredicate> klass;

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
        Predicate truePredicate = new TruePredicate();
        o.setPredicates(new Predicate[] {truePredicate});
        assertEquals(truePredicate, o.getPredicates()[0]);
    }

    @Test(expected = IllegalStateException.class)
    public void test_whenSetPredicatesOnExistingPredicates_thenThrowException()
            throws IllegalAccessException, InstantiationException {

        CompoundPredicate o = klass.newInstance();
        Predicate truePredicate = new TruePredicate();
        o.setPredicates(new Predicate[] {truePredicate});

        Predicate falsePredicate = new FalsePredicate();
        o.setPredicates(new Predicate[] {falsePredicate});

        fail();
    }

}
