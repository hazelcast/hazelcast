package com.hazelcast.map;

import org.junit.Test;

import java.lang.reflect.Constructor;

import static org.junit.Assert.assertEquals;

public class QueryResultSizeExceededExceptionTest {

    @Test
    public void testStringConstructor() throws Exception {
        QueryResultSizeExceededException exception = new QueryResultSizeExceededException();
        String expectedMessage = exception.getMessage();

        // invoke the constructor like in ClientInvocationServiceSupport.handleClientMessage()
        Class<?> causeClazz = Class.forName(QueryResultSizeExceededException.class.getName());
        Constructor<?> causeConstructor = causeClazz.getDeclaredConstructor(String.class);
        causeConstructor.setAccessible(true);
        Throwable actual = (Throwable) causeConstructor.newInstance(expectedMessage);

        assertEquals(expectedMessage, actual.getMessage());
    }
}
