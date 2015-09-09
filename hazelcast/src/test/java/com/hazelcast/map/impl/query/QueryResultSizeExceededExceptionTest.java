package com.hazelcast.map.impl.query;

import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
