/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientExceptionFactory.ExceptionFactory;
import com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
// It's called Simple, because the ClientExceptionFactoryTest is parametrized
public class ClientExceptionFactoryTestSimple {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ClientExceptionFactory exceptionFactory = new ClientExceptionFactory(false);

    private static class MyException extends Exception {
    }

    @Test
    public void testDuplicateErrorCode() {
        thrown.expect(HazelcastException.class);
        exceptionFactory.register(ClientProtocolErrorCodes.ARRAY_INDEX_OUT_OF_BOUNDS,
                MyException.class, new ExceptionFactory() {
                    @Override
                    public Throwable createException(String message, Throwable cause) {
                        return new MyException();
                    }
                });
    }

    @Test
    public void testDuplicateExceptionClass() {
        thrown.expect(HazelcastException.class);
        exceptionFactory.register(10000,
                ArrayIndexOutOfBoundsException.class, new ExceptionFactory() {
                    @Override
                    public Throwable createException(String message, Throwable cause) {
                        return new ArrayIndexOutOfBoundsException(message);
                    }
                });
    }

    @Test
    public void testIncorrectClassFromFactory() {
        thrown.expect(HazelcastException.class);
        exceptionFactory.register(10000,
                MyException.class, new ExceptionFactory() {
                    @Override
                    public Throwable createException(String message, Throwable cause) {
                        return new ArrayIndexOutOfBoundsException(message);
                    }
                });
    }

}
