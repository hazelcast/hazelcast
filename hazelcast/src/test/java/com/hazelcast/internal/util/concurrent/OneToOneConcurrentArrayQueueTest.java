package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OneToOneConcurrentArrayQueueTest extends AbstractConcurrentArrayQueueTest {

    @Before
    public void setUp()  {
        queue = new OneToOneConcurrentArrayQueue<Integer>(CAPACITY);
    }
}
