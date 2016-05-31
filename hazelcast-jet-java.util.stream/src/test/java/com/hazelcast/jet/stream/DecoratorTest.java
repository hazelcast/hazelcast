/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

@Category(QuickTest.class)
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
                    "stream")
    );

    @Test
    public void testListDecorator() throws Exception {
        HazelcastInstance instance = Mockito.mock(HazelcastInstance.class);
        assertDecorator(IList.class, IStreamList.class, l -> IStreamList.streamList(instance, l));
    }

    @Test
    public void testMapDecorator() throws Exception {
        HazelcastInstance instance = Mockito.mock(HazelcastInstance.class);
        assertDecorator(IMap.class, IStreamMap.class, m -> IStreamMap.streamMap(instance, m));
    }

    private <D, E extends D> void assertDecorator(Class<D> decorated, Class<E> decorator, Function<D, E> supplier) throws Exception {
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
        } else {
            return Mockito.mock(clazz);
        }
    }
}
