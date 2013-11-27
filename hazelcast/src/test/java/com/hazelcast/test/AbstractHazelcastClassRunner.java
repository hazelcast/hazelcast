package com.hazelcast.test;

import com.hazelcast.test.annotation.Repeat;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;

/**
 * User: ahmetmircik
 * Date: 11/27/13
 */
public abstract class AbstractHazelcastClassRunner extends BlockJUnit4ClassRunner{


    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}
     *
     * @throws org.junit.runners.model.InitializationError
     *          if the test class is malformed.
     */
    public AbstractHazelcastClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected Statement methodBlock(FrameworkMethod method) {
        final Statement statement = super.methodBlock(method);
        return whenRepeatPossible(method, statement);
    }

    protected Statement whenRepeatPossible(FrameworkMethod method, Statement next) {
        final Repeat repeatable = method.getAnnotation(Repeat.class);
        final int repeat = repeatable != null ? repeatable.value() : 1;
        return new HazelcastRepeater(next, method.getMethod(), repeat);
    }

    private class HazelcastRepeater extends Statement {

        private final Statement next;

        private final Method testMethod;

        private final int repeat;

        public HazelcastRepeater(Statement next, Method testMethod, int repeat) {
            this.next = next;
            this.testMethod = testMethod;
            this.repeat = Math.max(1, repeat);
        }

        /**
         * Invokes the next {@link Statement statement} in the execution chain for
         * the specified repeat count.
         */
        @Override
        public void evaluate() throws Throwable {
            for (int i = 0; i < this.repeat; i++) {
                if (this.repeat > 1) {
                    System.out.println(String.format("---> Repetable test [%s:%s], run count [%d]",
                            testMethod.getDeclaringClass().getCanonicalName(),
                            testMethod.getName(), i + 1));
                }
                this.next.evaluate();
            }
        }
    }
}
