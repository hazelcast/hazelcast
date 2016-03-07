package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.channels.Selector;

import static com.hazelcast.nio.tcp.nonblocking.SelectorOptimizer.findOptimizableSelectorClass;
import static com.hazelcast.test.HazelcastTestSupport.assertUtilityConstructor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SelectorOptimizerTest {
    private ILogger logger = Logger.getLogger(SelectionKeysSetTest.class);

    @Test
    public void testConstructor() {
        assertUtilityConstructor(SelectorOptimizer.class);
    }

    @Test
    public void optimize() throws Exception {
        Selector selector = Selector.open();
        assumeTrue(findOptimizableSelectorClass(selector) != null);
        SelectorOptimizer.SelectionKeysSet keys = SelectorOptimizer.optimize(selector, logger);
        assertNotNull(keys);
    }
}
