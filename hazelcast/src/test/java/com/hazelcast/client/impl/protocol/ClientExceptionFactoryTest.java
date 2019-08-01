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

import com.hazelcast.client.UndefinedErrorCodeException;
import com.hazelcast.client.impl.clientside.ClientExceptionFactory;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.CacheException;
import java.net.URISyntaxException;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExceptionFactoryTest extends HazelcastTestSupport {

    @Parameter
    public Throwable throwable;

    private ClientExceptions exceptions = new ClientExceptions(true);
    private ClientExceptionFactory exceptionFactory = new ClientExceptionFactory(true);

    @Test
    public void testException() {
        ClientMessage exceptionMessage = exceptions.createExceptionMessage(throwable);
        Throwable resurrectedThrowable = exceptionFactory.createException(exceptionMessage);

        if (!exceptionEquals(throwable, resurrectedThrowable)) {
            assertEquals(throwable, resurrectedThrowable);
        }
    }

    private boolean exceptionEquals(Throwable expected, Throwable actual) {
        if (expected == null && actual == null) {
            return true;
        }

        if (expected == null || actual == null) {
            return false;
        }

        if (exceptions.isKnownClass(expected.getClass())) {
            if (!expected.getClass().equals(actual.getClass())) {
                return false;
            }

            // We compare the message only for known exceptions.
            // We also ignore it for URISyntaxException, as it is not possible to restore it without special, probably JVM-version specific logic.
            if (expected.getClass() != URISyntaxException.class && !equals(expected.getMessage(), actual.getMessage())) {
                return false;
            }
        } else {
            if (!UndefinedErrorCodeException.class.equals(actual.getClass())
                    || !expected.getClass().getName().equals(((UndefinedErrorCodeException) actual).getOriginClassName())) {
                return false;
            }
        }

        if (!stackTraceArrayEquals(expected.getStackTrace(), actual.getStackTrace())) {
            return false;
        }

        // recursion to cause
        return exceptionEquals(expected.getCause(), actual.getCause());
    }

    // null-safe equals from Java 1.7
    private static boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }

    private boolean stackTraceArrayEquals(StackTraceElement[] stackTrace1, StackTraceElement[] stackTrace2) {
        assertEquals(stackTrace1.length, stackTrace2.length);
        for (int i = 0; i < stackTrace1.length; i++) {
            StackTraceElement stackTraceElement1 = stackTrace1[i];
            StackTraceElement stackTraceElement2 = stackTrace2[i];
            //Not using stackTraceElement.equals
            //because in IBM JDK stacktraceElements with null method name are not equal
            if (!equals(stackTraceElement1.getClassName(), stackTraceElement2.getClassName())
                    || !equals(stackTraceElement1.getMethodName(), stackTraceElement2.getMethodName())
                    || !equals(stackTraceElement1.getFileName(), stackTraceElement2.getFileName())
                    || !equals(stackTraceElement1.getLineNumber(), stackTraceElement2.getLineNumber())) {
                return false;
            }
        }
        return true;
    }

    @Parameters(name = "Throwable:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(
                new Object[]{new CacheException(randomString())},
                new Object[]{new ConsistencyLostException(randomString())}
        );
    }
}
