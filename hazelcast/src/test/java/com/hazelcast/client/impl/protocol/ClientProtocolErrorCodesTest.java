/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.impl.clientside.ClientExceptionFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientProtocolErrorCodesTest extends HazelcastTestSupport {
    private static final String dummyIoErrorMessage = "dummy io error";

    public static class MyException extends Exception {
        public MyException(String message, Throwable cause) {
            super(message, cause);
        }

        @Override
        public boolean equals(Object obj) {
            if (!obj.getClass().equals(MyException.class)) {
                return false;
            }

            MyException other = (MyException) obj;
            return getMessage().equals(other.getMessage()) && other.getCause().getClass().equals(IOException.class) && other
                    .getCause().getMessage().equals(dummyIoErrorMessage);
        }
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientProtocolErrorCodes.class);
    }

    @Test
    public void testUserException() {
        ClientExceptions exceptions = new ClientExceptions(false);
        ClientExceptionFactory exceptionFactory = new ClientExceptionFactory(getClass().getClassLoader());

        MyException userThrowable = new MyException("User exception with a cause", new IOException(dummyIoErrorMessage));
        ClientMessage exceptionMessage = exceptions.createExceptionMessage(userThrowable);
        Throwable resurrectedThrowable = exceptionFactory.createException(exceptionMessage);
        assertEquals(userThrowable, resurrectedThrowable);
    }
}
