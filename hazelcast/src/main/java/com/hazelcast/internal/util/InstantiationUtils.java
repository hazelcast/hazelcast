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

package com.hazelcast.internal.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 * Convenience for class instantiation.
 *
 */
public final class InstantiationUtils {
    private InstantiationUtils() {

    }

    /**
     * Create a new instance of a given class. It will search for a constructor matching passed parameters.
     * If a matching constructor is not found then it returns null.
     *
     * Constructor is matching when it can be invoked with given parameters. The order of parameters is significant.
     *
     * When a class constructor contains a primitive argument then it's matching if and only if
     * a parameter at the same position is not null.
     *
     * It throws {@link AmbigiousInstantiationException} when multiple matching constructors are found.
     *
     * @param clazz class to be instantiated
     * @param params parameters to be passed to the constructor
     * @param <T> class type to be instantiated
     * @return a new instance of a given class
     * @throws AmbigiousInstantiationException when multiple constructors matching the parameters
     */
    public static <T> T newInstanceOrNull(Class<? extends T> clazz, Object...params)  {
        Class[] paramTypes = new Class[params.length];
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            paramTypes[i] = param == null ? null : param.getClass();
        }
        Constructor<T> constructor = selectMatchingConstructor(clazz, params, paramTypes);
        if (constructor == null) {
            return null;
        }
        try {
            return constructor.newInstance(params);
        } catch (IllegalAccessException e) {
            return null;
        } catch (InstantiationException e) {
            return null;
        } catch (InvocationTargetException e) {
            return null;
        }
    }

    private static <T> Constructor<T> selectMatchingConstructor(Class<? extends T> clazz, Object[] params,
                                                                Class<?>[] paramTypes) {
        Constructor<?>[] constructors = clazz.getConstructors();
        Constructor<T> selectedConstructor = null;
        for (Constructor<?> constructor : constructors) {
            if (isParamsMatching(constructor, params, paramTypes)) {
                if (selectedConstructor == null) {
                    selectedConstructor = (Constructor<T>) constructor;
                } else {
                    throw new AmbigiousInstantiationException("Class " + clazz + " has multiple constructors matching "
                            + "given parameter types: " + Arrays.toString(paramTypes));
                }
            }
        }
        return selectedConstructor;
    }

    private static boolean isParamsMatching(Constructor<?> constructor, Object[] params, Class[] paramTypes) {
        Class<?>[] constructorParamTypes = constructor.getParameterTypes();
        if (constructorParamTypes.length != paramTypes.length) {
            return false;
        }
        for (int i = 0; i < constructorParamTypes.length; i++) {
            Class<?> constructorParamType = constructorParamTypes[i];
            if (constructorParamType.isPrimitive()) {
                if (params[i] == null) {
                    // passed argument was null, but the argument in constructor is primitive - it's not matching
                    return false;
                } else {
                    constructorParamType = toBoxedType(constructorParamType);
                }
            }
            Class paramType = paramTypes[i];
            if (paramType != null && !constructorParamType.isAssignableFrom(paramType)) {
                return false;
            }
        }
        return true;
    }

    private static Class<?> toBoxedType(Class<?> type) {
        assert type.isPrimitive();
        if (type == boolean.class) {
            return Boolean.class;
        } else if (type == byte.class) {
            return Byte.class;
        } else if (type == char.class) {
            return Character.class;
        } else if (type == double.class) {
            return Double.class;
        } else if (type == float.class) {
            return Float.class;
        } else if (type == int.class) {
            return Integer.class;
        } else if (type == long.class) {
            return Long.class;
        } else if (type == short.class) {
            return Short.class;
        } else {
            // should never happen ;-)
            throw new IllegalArgumentException("Unknown primitive type " + type.getName());
        }
    }
}
