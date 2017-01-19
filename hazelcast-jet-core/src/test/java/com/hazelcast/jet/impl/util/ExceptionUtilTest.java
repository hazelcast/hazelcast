package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestInstanceFactory;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.TestUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ExceptionUtilTest extends JetTestSupport {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void when_throwableIsRuntimeException_then_peelReturnsOriginal() {
        Throwable throwable = new RuntimeException("expected exception");
        Throwable result = peel(throwable);

        assertEquals(throwable, result);
    }

    @Test
    public void when_throwableIsExecutionException_then_peelReturnsCause() {
        Throwable throwable = new RuntimeException("expected exception");
        Throwable result = peel(new ExecutionException(throwable));

        assertEquals(throwable, result);
    }

    @Test
    public void when_throwableIsExecutionExceptionWithNullCause_then_returnHazelcastException() {
        ExecutionException exception = new ExecutionException(null);
        exceptionRule.expect(JetException.class);
        throw rethrow(exception);
    }

    @Test
    public void test_serializationFromNodeToClient() throws InterruptedException {
        // create one member and one client
        createJetMember();
        JetInstance client = createJetClient();

        try {
            DAG dag = new DAG();
            dag.newVertex("source", ErrorGenerator::new).localParallelism(1);
            client.newJob(dag).execute().get();
        } catch (ExecutionException caught) {
            assertThat(caught.toString(), containsString(ErrorGenerator.newException().toString()));
            TestUtil.assertExceptionInCauses(ErrorGenerator.newException(), caught);
        } finally {
            shutdownFactory();
        }
    }

    @Test
    public void test_serializationOnNode() throws InterruptedException {
        JetTestInstanceFactory factory = new JetTestInstanceFactory();
        // create one member and one client
        JetInstance client = factory.newMember();

        try {
            DAG dag = new DAG();
            dag.newVertex("source", ErrorGenerator::new).localParallelism(1);
            client.newJob(dag).execute().get();
        } catch (ExecutionException caught) {
            assertThat(caught.toString(), containsString(ErrorGenerator.newException().toString()));
            TestUtil.assertExceptionInCauses(ErrorGenerator.newException(), caught);
        } finally {
            factory.shutdownAll();
        }
    }

    public static class ErrorGenerator implements Processor {
        @Override
        public boolean complete() {
            throw newException();
        }

        private static RuntimeException newException() {
            return new RuntimeException("myException");
        }
    }

}
