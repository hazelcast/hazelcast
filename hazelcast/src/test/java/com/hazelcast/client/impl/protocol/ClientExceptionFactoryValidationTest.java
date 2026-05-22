/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.clientside.ClientLoggingService;
import com.hazelcast.client.impl.protocol.codec.builtin.ErrorsCodec;
import com.hazelcast.client.impl.protocol.exception.ErrorHolder;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.client.impl.protocol.ClientProtocolErrorCodes.RUNTIME;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExceptionFactoryValidationTest extends HazelcastTestSupport {

    private final ClientExceptionFactory exceptionFactory = new ClientExceptionFactory(true,
            Thread.currentThread().getContextClassLoader(),
            new ClientLoggingService("dummy", "jdk", BuildInfoProvider.getBuildInfo(), "i1", true, false));

    // this is a fallback check in case exception thrown by the constructor is swallowed
    static boolean constructorCalled = false;

    @Parameter
    public Class<?> throwable;

    @Parameters(name = "Throwable:{0}")
    public static Iterable<Class<?>> parameters() {
        return asList(TestNonException.class, TestNonExceptionMessage.class);
    }

    @Before
    public void setUp() {
        constructorCalled = false;
    }

    @Test
    public void nonExceptionShouldBeRestoredAsUndefined() {
        List<ErrorHolder> errorHolders = new LinkedList<>();
        errorHolders.add(new ErrorHolder(0, throwable.getName(), "msg", List.of()));
        ClientMessage exceptionMessage = ErrorsCodec.encode(errorHolders);

        Throwable exception = exceptionFactory.createException(exceptionMessage);
        UndefinedErrorCodeException expected = new UndefinedErrorCodeException(
                "Received exception class that is not Throwable: msg", throwable.getName(), null);
        assertThat(exception)
                // do not compare stacktraces
                .isInstanceOf(expected.getClass())
                .hasMessage(expected.getMessage());
        assertThat(constructorCalled).isFalse();
    }

    @Test
    public void nestedNonExceptionShouldBeRestoredAsUndefined() {
        List<ErrorHolder> errorHolders = new LinkedList<>();
        errorHolders.add(new ErrorHolder(RUNTIME, RuntimeException.class.getName(), "runtime", List.of()));
        errorHolders.add(new ErrorHolder(0, throwable.getName(), "msg", List.of()));
        ClientMessage exceptionMessage = ErrorsCodec.encode(errorHolders);

        Throwable exception = exceptionFactory.createException(exceptionMessage);
        UndefinedErrorCodeException expected = new UndefinedErrorCodeException(
                "Received exception class that is not Throwable: msg", throwable.getName(), null);
        assertThat(exception)
                .isInstanceOf(RuntimeException.class)
                .rootCause()
                // do not compare stacktraces
                .isInstanceOf(expected.getClass())
                .hasMessage(expected.getMessage());
        assertThat(constructorCalled).isFalse();
    }

    public static class TestNonException {
        public TestNonException() {
            fail("constructor should not be called");
            constructorCalled = true;
        }
    }

    public static class TestNonExceptionMessage {
        public TestNonExceptionMessage(String message) {
            fail("constructor should not be called");
            constructorCalled = true;
        }
    }
}
