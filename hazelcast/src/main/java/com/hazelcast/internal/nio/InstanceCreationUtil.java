/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Utility class to deal with creation of new object instances.
 */
public final class InstanceCreationUtil {

    private static final Objenesis OBJENESIS = new ObjenesisStd();
    private static final Map<Class<?>, ObjectInstantiator<?>> OBJECT_INSTANTIATORS = new ConcurrentHashMap<>();

    private InstanceCreationUtil() {
    }


    /**
     * Creates a new instance of given class, bypassing constructors using Objenesis' instantiators.
     * @param klass class which instance will be created
     * @param <T> type of class {@code klass}
     * @return newly created blank object
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> T newInstanceBypassingConstructor(Class klass) {
        ObjectInstantiator instantiator = OBJECT_INSTANTIATORS.computeIfAbsent(klass, OBJENESIS::getInstantiatorOf);
        return (T) instantiator.newInstance();
    }
}
