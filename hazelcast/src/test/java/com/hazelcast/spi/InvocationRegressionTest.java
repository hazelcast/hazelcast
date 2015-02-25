package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationRegressionTest extends HazelcastTestSupport {
    @Test(expected = ExecutionException.class, timeout = 120000)
    public void testIssue2509() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();

        UnDeserializable unDeserializable = new UnDeserializable(1);
        IExecutorService executorService = h1.getExecutorService("default");
        Issue2509Runnable task = new Issue2509Runnable(unDeserializable);
        Future<?> future = executorService.submitToMember(task, h2.getCluster().getLocalMember());
        future.get();
    }

    public static class Issue2509Runnable implements Callable<Integer>, DataSerializable {

        private UnDeserializable unDeserializable;

        public Issue2509Runnable() {
        }

        public Issue2509Runnable(UnDeserializable unDeserializable) {
            this.unDeserializable = unDeserializable;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(unDeserializable);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            unDeserializable = in.readObject();
        }

        @Override
        public Integer call() {
            return unDeserializable.foo;
        }
    }

    public static class UnDeserializable implements DataSerializable {

        private int foo;

        public UnDeserializable(int foo) {
            this.foo = foo;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(foo);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            foo = in.readInt();
        }
    }

}
