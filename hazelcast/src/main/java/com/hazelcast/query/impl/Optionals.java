/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.hazelcast.util.ExceptionUtil.rethrow;

@SuppressWarnings("unchecked")
public final class Optionals {

    private static final Class OPTIONAL;
    private static final Method IS_PRESENT;
    private static final Method GET;

    static {
        Class optional;
        Method isPresent;
        Method get;

        try {
            optional = Class.forName("java.util.Optional");

            isPresent = optional.getDeclaredMethod("isPresent");
            isPresent.setAccessible(true);

            get = optional.getDeclaredMethod("get");
            get.setAccessible(true);
        } catch (ClassNotFoundException e) {
            optional = null;
            isPresent = null;
            get = null;
        } catch (NoSuchMethodException e) {
            optional = null;
            isPresent = null;
            get = null;
        }

        OPTIONAL = optional;
        IS_PRESENT = isPresent;
        GET = get;
    }

    private Optionals() {
    }

    public static Object unwrapIfOptional(Object object) {
        if (object == null || object.getClass() != OPTIONAL) {
            return object;
        }

        try {
            return (Boolean) IS_PRESENT.invoke(object) ? GET.invoke(object) : null;
        } catch (IllegalAccessException e) {
            throw rethrow(e);
        } catch (InvocationTargetException e) {
            throw rethrow(e);
        }
    }

}
