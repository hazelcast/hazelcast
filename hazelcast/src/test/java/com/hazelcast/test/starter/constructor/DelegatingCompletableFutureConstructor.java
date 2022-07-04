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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.starter.HazelcastProxyFactory;
import com.hazelcast.test.starter.HazelcastStarterConstructor;
import com.hazelcast.test.starter.ReflectionUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

@HazelcastStarterConstructor(classNames = "com.hazelcast.spi.impl.DelegatingCompletableFuture")
public class DelegatingCompletableFutureConstructor extends AbstractStarterObjectConstructor {

    public DelegatingCompletableFutureConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader classLoader = targetClass.getClassLoader();
        Class<?> serializationServiceClass = classLoader.loadClass(SerializationService.class.getName());
        MethodType ctorType = MethodType.methodType(void.class, serializationServiceClass, CompletableFuture.class);

        MethodHandle constructor = PUBLIC_LOOKUP.findConstructor(targetClass, ctorType);

        Object serializationService = HazelcastProxyFactory.proxyArgumentsIfNeeded(
                new Object[] {ReflectionUtils.getFieldValueReflectively(delegate, "serializationService")},
                serializationServiceClass.getClassLoader())[0];
        try {
            return constructor.invoke(serializationService, delegate);
        } catch (Throwable throwable) {
            throw sneakyThrow(throwable);
        }
    }
}
