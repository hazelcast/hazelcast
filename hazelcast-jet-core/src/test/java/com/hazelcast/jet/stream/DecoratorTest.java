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

package com.hazelcast.jet.stream;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.stream.impl.ListDecorator;
import com.hazelcast.jet.stream.impl.MapDecorator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@RunWith(HazelcastParallelClassRunner.class)
public class DecoratorTest {

    private static final Set<String> EXCEPTIONS = new HashSet<>(
            Arrays.asList(
                    "compute",
                    "computeIfAbsent",
                    "computeIfPresent",
                    "getOrDefault",
                    "forEach",
                    "equals",
                    "hashCode",
                    "merge",
                    "parallelStream",
                    "replaceAll",
                    "removeIf",
                    "sort",
                    "spliterator",
                    "stream",
                    "listIterator"
                    )
    );

    @Test
    public void listDecorator() throws Exception {
        JetInstance instance = Mockito.mock(JetInstance.class);
        assertDecorator(IList.class, IStreamList.class, l -> new ListDecorator<>(l, instance));
    }

    @Test
    public void mapDecorator() throws Exception {
        JetInstance instance = Mockito.mock(JetInstance.class);
        assertDecorator(IMap.class, IStreamMap.class, m -> new MapDecorator<>(m, instance));
    }

    private <D, E extends D> void assertDecorator(Class<D> decorated, Class<E> decorator, Function<D, E> supplier)
    throws Exception {
        for (Method method : decorated.getMethods()) {
            if (EXCEPTIONS.contains(method.getName())) {
                continue;
            }
            try {
                D mock = Mockito.mock(decorated);
                E decoratorInstance = supplier.apply(mock);

                Class<?>[] parameterTypes = method.getParameterTypes();
                Method decoratorMethod = decorator.getMethod(method.getName(), parameterTypes);

                Object[] args = new Object[parameterTypes.length];
                for (int i = 0; i < parameterTypes.length; i++) {
                    Class<?> clazz = parameterTypes[i];
                    args[i] = getMockedValue(clazz);
                }
                // mock the return value, because some methods have @Nonnull annotations
                if (method.getReturnType() != void.class) {
                    // get the overridden return type, it might be more specific than in superclass
                    Class<?> returnType = decorated.getMethod(method.getName(), parameterTypes).getReturnType();
                    Mockito.when(method.invoke(mock, args)).thenReturn(getMockedValue(returnType));
                }
                decoratorMethod.invoke(decoratorInstance, args);
                method.invoke(Mockito.verify(mock), args);
            } catch (Exception e) {
                System.out.println("Could not verify " + method);
                throw e;
            }
        }
    }

    private Object getMockedValue(Class<?> clazz) {
        if (clazz == int.class) {
            return 0;
        } else if (clazz == long.class) {
            return 0L;
        } else if (clazz == boolean.class) {
            return false;
        } else if (clazz == Object[].class) {
            return new Object[0];
        } else if (clazz == String.class) {
            return "";
        } else if (clazz.equals(TimeUnit.class)) {
           return TimeUnit.MILLISECONDS;
        } else {
            return Mockito.mock(clazz);
        }
    }
}
