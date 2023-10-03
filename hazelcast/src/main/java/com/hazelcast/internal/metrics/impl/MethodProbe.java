/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeFunction;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * A {@link MethodProbe} is a {@link ProbeFunction} that invokes a method that is annotated with {@link Probe}.
 */
abstract class MethodProbe<S> extends MethodHandleProbe<S> {
    protected MethodProbe(MethodHandle getterMethod, boolean isStatic, Probe probe, ProbeType type,
            SourceMetadata sourceMetadata) {
        super(getterMethod, isStatic, probe, type, sourceMetadata);
    }

    static <S> MethodHandleProbe<S> createProbe(Method method, Probe probe, SourceMetadata sourceMetadata) {
        try {
            method.setAccessible(true);
            return createProbe(LOOKUP.unreflect(method), Modifier.isStatic(method.getModifiers()), probe, sourceMetadata);
        } catch (final IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
