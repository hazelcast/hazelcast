package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.UndefinedErrorCodeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientProtocolErrorCodesTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientProtocolErrorCodes.class);
    }

    @Test
    public void testUndefinedException() {
        ClientExceptionFactory exceptionFactory = new ClientExceptionFactory(false);
        class MyException extends Exception {
        }

        ClientMessage exceptionMessage = exceptionFactory.createExceptionMessage(new MyException());
        ClientMessage responseMessage = ClientMessage.createForDecode(exceptionMessage.buffer(), 0);
        Throwable resurrectedThrowable = exceptionFactory.createException(responseMessage);
        assertEquals(UndefinedErrorCodeException.class, resurrectedThrowable.getClass());
    }
}
