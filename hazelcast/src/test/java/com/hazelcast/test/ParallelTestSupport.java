package com.hazelcast.test;

import org.junit.After;
import org.junit.runner.RunWith;

/**
 * @mdogan 5/24/13
 */

@RunWith(RandomBlockJUnit4ClassRunner.class)
public abstract class ParallelTestSupport {

    private StaticNodeFactory factory;

    protected final StaticNodeFactory createNodeFactory(int nodeCount) {
        if (factory != null) {
            throw new IllegalStateException("Node factory is already created!");
        }
        return factory = new StaticNodeFactory(nodeCount);
    }

    @After
    public final void shutdownNodeFactory() {
        final StaticNodeFactory f = factory;
        if (f != null) {
            factory = null;
            f.shutdownAll();
        }
    }
}
