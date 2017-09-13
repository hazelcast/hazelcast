package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;

/**
 * Checks if the Executor deals correctly with deserialization issues of the task.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExecutorSerializationErrorTest extends HazelcastTestSupport {

    private HazelcastInstance local;
    private HazelcastInstance remote;
    private IExecutorService executor;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(cluster);
        local = cluster[0];
        remote = cluster[1];
        executor = local.getExecutorService("foo");
    }

    @Test
    public void test() throws Exception {
        Future f = executor.submitToMember(new BrokenCallable(), remote.getCluster().getLocalMember());

        assertCompletesEventually(f);

        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertInstanceOf(HazelcastSerializationException.class, e.getCause());
            assertInstanceOf(ExpectedRuntimeException.class, e.getCause().getCause());
        }
    }

    private static class BrokenCallable implements DataSerializable, Callable {
        @Override
        public Object call() throws Exception {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new ExpectedRuntimeException();
        }
    }
}
