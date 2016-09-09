package com.hazelcast.cardinality.hyperloglog;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICardinalityEstimator;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class HyperLogLogAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;

    private ICardinalityEstimator estimator;

    @Before
    public void setup() {
        instances = newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        estimator = local.getCardinalityEstimator(name);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test
    public void testAggregateHash() {
        for (int l=0; l<50000; l++) {
            // Sparse store will always return true to changed due to its design.
            assertEquals(true, estimator.aggregateHash((long) l));
        }

        // Dense store now.
        assertEquals(false, estimator.aggregateHash(1L));
        assertEquals(false, estimator.aggregateHash(2L));
        assertEquals(false, estimator.aggregateHash(1000L));
        assertEquals(false, estimator.aggregateHash(40000L));
    }

    @Test
    public void testAggregateHashes() {
        long[] hashes = new long[50000];
        for (int l=0; l<50000; l++) {
            hashes[l] = (long) l;
        }

        // Sparse store will always return true to changed due to its design.
        assertEquals(true, estimator.aggregateHashes(hashes));

        // Dense store now.
        assertEquals(false, estimator.aggregateHashes(hashes));
    }
}
