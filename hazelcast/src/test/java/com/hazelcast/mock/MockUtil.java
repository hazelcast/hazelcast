/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mock;

import org.mockito.AdditionalAnswers;
import org.mockito.MockSettings;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.withSettings;

public class MockUtil {

    @SuppressWarnings("unchecked")
    /** Delegate calls to another object **/
    public static <T> Answer<T> delegateTo(Object delegate) {
        return (Answer<T>) new DelegatingAnswer(delegate);
    }

    /**
     * Creates a Mockito spy which is Serializable
     **/
    public static <T> T serializableSpy(Class<T> clazz, T instance) {
        MockSettings settings = withSettings().spiedInstance(instance).serializable().defaultAnswer(CALLS_REAL_METHODS);
        return Mockito.mock(clazz, settings);
    }

    /**
     * Mockito Answer that delegates invocations to another object. Similar to
     * {@link AdditionalAnswers#delegatesTo(Object)} but also supports objects
     * of different Class.
     **/
    static class DelegatingAnswer implements Answer<Object> {

        private Object delegated;

        DelegatingAnswer(Object delegated) {
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
