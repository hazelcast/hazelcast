package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICardinalityEstimator;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

public abstract class HyperLogLogAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;

    private ICardinalityEstimator estimator;

    @Before
    public void setup() {
        instances = newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        estimator = local.getHyperLogLog(name);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test
    public void testAddHash() {
        estimator.addHash(1L);
    }

}
