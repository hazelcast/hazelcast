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

package com.hazelcast.internal.nio;

import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Utility class to deal with creation of new object instances.
 */
public final class InstanceCreationUtil {

    private static final Objenesis OBJENESIS = new ObjenesisStd(true);
    private static final Map<Class<?>, Supplier<?>> OBJECT_SUPPLIERS = new ConcurrentHashMap<>();

    private InstanceCreationUtil() {
    }


    /**
     * Creates a new instance of given class, bypassing constructors using Objenesis' instantiators if the default
     * constructor does not exist.
     * @param klass class which instance will be created
     * @param <T> type of class {@code klass}
     * @return newly created blank object
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> T createNewInstance(Class klass) {
        Supplier<T> supplier = (Supplier<T>) OBJECT_SUPPLIERS.computeIfAbsent(klass, clz -> {
           if (canUseConstructor(clz)) {
               return () -> (T) newInstanceUsingConstructor(clz);
           } else {
               ObjectInstantiator instantiator = OBJENESIS.getInstantiatorOf(klass);
               return () -> (T) instantiator.newInstance();
           }
        });

        return supplier.get();
    }

    private static boolean canUseConstructor(Class<?> clz) {
        try {
            ClassLoaderUtil.newInstance(clz.getClassLoader(), clz);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    private static <T> T newInstanceUsingConstructor(Class<T> clz) {
        try {
            return ClassLoaderUtil.newInstance(clz.getClassLoader(), clz);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
