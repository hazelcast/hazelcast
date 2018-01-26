/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.MockingDetails;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPrivate;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapOperationProviderDelegatorTest extends HazelcastTestSupport {

    private MapOperationProvider operationProvider;
    private TestMapOperationProviderDelegator delegator;

    @Before
    public void setUp() {
        operationProvider = mock(MapOperationProvider.class);
        delegator = new TestMapOperationProviderDelegator();
    }

    @Test
    public void testDelegator() throws Exception {
        Method[] methods = MapOperationProviderDelegator.class.getDeclaredMethods();

        // invoke all methods of MapOperationProviderDelegator
        for (Method method : methods) {
            if (isAbstract(method.getModifiers()) || isPrivate(method.getModifiers())) {
                continue;
            }

            Object[] parameters = getParameters(method);
            try {
                method.invoke(delegator, parameters);
            } catch (Exception e) {
                System.err.println(format("Could not invoke method %s: %s", method.getName(), e.getMessage()));
            }
        }

        // get a list of all method invocations from Mockito
        List<String> methodsCalled = new ArrayList<String>();
        MockingDetails mockingDetails = Mockito.mockingDetails(operationProvider);
        Collection<Invocation> invocations = mockingDetails.getInvocations();
        for (Invocation invocation : invocations) {
            methodsCalled.add(invocation.getMethod().getName());
        }

        // verify that all methods have been called on the delegated MapOperationProvider
        for (Method method : methods) {
            if (isAbstract(method.getModifiers()) || isPrivate(method.getModifiers())) {
                continue;
            }
            assertTrue(format("Method %s() should have been called", method.getName()), methodsCalled.contains(method.getName()));
        }
    }

    private Object[] getParameters(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] parameters = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            if (parameterType.isAssignableFrom(String.class)) {
                parameters[i] = "test";
            } else if (parameterType.isAssignableFrom(Long.class) || parameterType.equals(long.class)) {
                parameters[i] = Long.MAX_VALUE;
            } else if (parameterType.isAssignableFrom(Integer.class) || parameterType.equals(int.class)) {
                parameters[i] = Integer.MAX_VALUE;
            } else if (parameterType.isAssignableFrom(Short.class) || parameterType.equals(short.class)) {
                parameters[i] = Short.MAX_VALUE;
            } else if (parameterType.isAssignableFrom(Byte.class) || parameterType.equals(byte.class)) {
                parameters[i] = Byte.MAX_VALUE;
            } else if (parameterType.isAssignableFrom(Boolean.class) || parameterType.equals(boolean.class)) {
                parameters[i] = true;
            } else if (parameterType.equals(long[].class)) {
                parameters[i] = new long[0];
            } else if (parameterType.equals(int[].class)) {
                parameters[i] = new int[0];
            } else if (parameterType.equals(short[].class)) {
                parameters[i] = new short[0];
            } else if (parameterType.equals(byte[].class)) {
                parameters[i] = new byte[0];
            } else if (parameterType.equals(boolean[].class)) {
                parameters[i] = new boolean[0];
            } else if (parameterType.isAssignableFrom(MapEntries.class)) {
                parameters[i] = new MapEntries();
            } else if (parameterType.equals(MapEntries[].class)) {
                parameters[i] = new MapEntries[0];
            } else {
                try {
                    parameters[i] = mock(parameterType);
                } catch (Exception e) {
                    throw new IllegalStateException(format(
                            "Cannot mock parameter of type %s of MapOperationProviderDelegator.%s() at position %d: %s",
                            parameterType.getSimpleName(), method.getName(), i, e.getMessage()));
                }
            }
        }
        return parameters;
    }

    private class TestMapOperationProviderDelegator extends MapOperationProviderDelegator {

        @Override
        MapOperationProvider getDelegate() {
            return operationProvider;
        }
    }
}
