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

package com.hazelcast.internal.adapter;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.lang.reflect.Method;
import java.util.Arrays;

import static java.lang.String.format;

/**
 * Checks if the given {@link DataStructureAdapter} class implements a specified method.
 */
public final class MethodAvailableMatcher extends TypeSafeMatcher<Class<? extends DataStructureAdapter>> {

    private static final ILogger LOGGER = Logger.getLogger(MethodAvailableMatcher.class);

    private final DataStructureAdapterMethod adapterMethod;
    private final String methodName;
    private final String parameterTypeString;

    public MethodAvailableMatcher(DataStructureAdapterMethod method) {
        this.adapterMethod = method;
        this.methodName = method.getMethodName();
        this.parameterTypeString = method.getParameterTypeString();
    }

    /**
     * Matches the given {@link DataStructureAdapter} class with the specified method name and parameter types.
     *
     * @param dataStructureAdapterClass the {@link DataStructureAdapter} class to test
     * @return {@code true} if the method is found and is not annotated with {@link MethodNotAvailable}
     */
    @Override
    public boolean matchesSafely(Class<? extends DataStructureAdapter> dataStructureAdapterClass) {
        try {
            Method method = dataStructureAdapterClass.getMethod(methodName, adapterMethod.getParameterTypes());
            boolean isAvailable = !method.isAnnotationPresent(MethodNotAvailable.class);
            LOGGER.info(format("%s.%s(%s) is available: %b (%s)!", dataStructureAdapterClass.getSimpleName(), methodName,
                    parameterTypeString, isAvailable, Arrays.toString(method.getAnnotations())));
            return isAvailable;
        } catch (Throwable t) {
            throw new AssertionError(format("Could not find method %s.%s(%s): %s", dataStructureAdapterClass.getSimpleName(),
                    methodName, parameterTypeString, t.getMessage()));
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(format("%s(%s) to be available", methodName, parameterTypeString));
    }

    @Override
    protected void describeMismatchSafely(Class<? extends DataStructureAdapter> dataStructureAdapterClass,
                                          Description mismatchDescription) {
        mismatchDescription.appendText(format("%s.%s(%s) is annotated with @%s", dataStructureAdapterClass.getSimpleName(),
                methodName, parameterTypeString, MethodNotAvailable.class.getSimpleName()));
    }
}
