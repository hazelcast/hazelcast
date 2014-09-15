package com.hazelcast.mock;

import java.lang.reflect.Method;

import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class MockUtil {

    @SuppressWarnings("unchecked")
    /** Delegate calls to another object **/
    public static <T> Answer<T> delegateTo(Object delegate) {
        return (Answer<T>) new DelegatingAnswer(delegate);
    }

    /**
     * Mockito Answer that delegates invocations to another object. Similar to
     * {@link AdditionalAnswers#delegatesTo(Object)} but also supports objects
     * of different Class.
     **/
    static class DelegatingAnswer implements Answer<Object> {

        private Object delegated;

        public DelegatingAnswer(Object delegated) {
            this.delegated = delegated;
        }

        @Override
        public Object answer(InvocationOnMock inv) throws Throwable {
            Method m = inv.getMethod();
            Method rm = delegated.getClass().getMethod(m.getName(),
                    m.getParameterTypes());
            return rm.invoke(delegated, inv.getArguments());
        }
    }
}
