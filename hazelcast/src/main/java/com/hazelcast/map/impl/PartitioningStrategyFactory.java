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

package com.hazelcast.map.impl;

import com.hazelcast.config.FunctionArgument;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.partition.PartitioningStrategy;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Factory for {@link PartitioningStrategy} instances.
 */
public final class PartitioningStrategyFactory {
    // not private for tests
    final ConcurrentHashMap<String, PartitioningStrategy> cache = new ConcurrentHashMap<>();

    // this is set to the current NodeEngine.getConfigClassLoader
    private final ClassLoader configClassLoader;

    /**
     * Construct a new PartitioningStrategyFactory
     *
     * @param configClassLoader the current {@code NodeEngine}'s {@code configClassLoader}.
     */
    public PartitioningStrategyFactory(ClassLoader configClassLoader) {
        this.configClassLoader = configClassLoader;
    }

    /**
     * Obtain a {@link PartitioningStrategy} for the given {@code NodeEngine} and {@code mapName}. This method
     * first attempts locating a {@link PartitioningStrategy} in {code config.getPartitioningStrategy()}. If this is {@code null},
     * then looks up its internal cache of partitioning strategies; if one has already been created for the given
     * {@code mapName}, it is returned, otherwise it is instantiated, cached and returned.
     *
     * @param mapName           Map for which this partitioning strategy is being created
     * @param config            the partitioning strategy configuration
     * @param strategyArguments
     * @return
     */
    @SuppressWarnings("checkstyle:NestedIfDepth")
    public PartitioningStrategy getPartitioningStrategy(
            String mapName,
            PartitioningStrategyConfig config,
            List<FunctionArgument> strategyArguments
    ) {
        PartitioningStrategy strategy = null;
        if (config != null) {
            strategy = config.getPartitioningStrategy();
            if (strategy == null) {
                if (cache.containsKey(mapName)) {
                    strategy = cache.get(mapName);
                } else if (config.getPartitioningStrategyClass() != null) {
                    try {
                        if (strategyArguments != null && !strategyArguments.isEmpty()) {
                            strategy = constructPartitioningStrategyWithArgs(
                                    config.getPartitioningStrategyClass(),
                                    strategyArguments
                            );
                        } else {
                            strategy = ClassLoaderUtil.newInstance(configClassLoader, config.getPartitioningStrategyClass());
                        }
                        cache.put(mapName, strategy);
                    } catch (Exception e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                }
            }
        }
        return strategy;
    }

    /**
     * Remove the cached {@code PartitioningStrategy} from the internal cache, if it exists.
     *
     * @param mapName name of the map whose partitioning strategy will be removed from internal cache
     */
    public void removePartitioningStrategyFromCache(String mapName) {
        cache.remove(mapName);
    }

    private PartitioningStrategy constructPartitioningStrategyWithArgs(
            final String className,
            final List<FunctionArgument> arguments
    ) throws ClassNotFoundException {
        final Class<?> strategyClass = ClassLoaderUtil.loadClass(configClassLoader, className);
        if (!PartitioningStrategy.class.isAssignableFrom(strategyClass)) {
            throw new HazelcastException("Provided PartitionStrategy class "
                    + "does not implement PartitionStrategy interface: " + strategyClass.getName());
        }

        final List<Class<?>> argumentTypes = arguments.stream()
                .map(functionArgument ->
                        convertTypeNameToClass(functionArgument.getTypeName(), functionArgument.isArray())
                ).collect(Collectors.toList());

        final Constructor<?> constructor = findFittingConstructor(strategyClass, argumentTypes);

        final Object[] converted = arguments.stream()
                .map(arg -> {
                    final Class<?> targetClass = convertTypeNameToClass(arg.getTypeName(), arg.isArray());
                    if (arg.isArray()) {
                        final Object value = convertArray(arg.getTypeName(), arg.getValues(), targetClass);
                        return targetClass.cast(value);
                    } else {
                        if (arg.getValues().length == 0 || arg.getValues()[0] == null || arg.getValues()[0].isEmpty()) {
                            return targetClass.cast(null);
                        }
                        final Object value = convertValue(arg.getTypeName(), arg.getValues()[0]);
                        return targetClass.cast(value);
                    }
                })
                .toArray();

        try {
            return (PartitioningStrategy) constructor.newInstance(converted);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new HazelcastException("Failed to instantiate PartitionStrategy with constructor " + constructor, e);
        }
    }

    private Constructor<?> findFittingConstructor(final Class<?> strategyClass, final List<Class<?>> argumentTypes) {
        Constructor<?> constructor = null;

        for (final Constructor<?> current : strategyClass.getConstructors()) {
            boolean fits = true;

            // TODO: match by name/index?
            for (int i = 0; i < current.getParameterTypes().length; i++) {
                final Class<?> constructorArgType = current.getParameterTypes()[i];
                if (constructorArgType.isAssignableFrom(argumentTypes.get(i))) {
                    continue;
                }

                fits = false;
                break;
            }

            if (fits) {
                constructor = current;
                break;
            }
        }
        if (constructor == null) {
            throw new HazelcastException("Could not find fitting constructor for specified PartitionStrategy: "
                    + strategyClass.getName());
        }

        return constructor;
    }

    private Object convertArray(String typeName, String[] values, Class<?> targetClass) {
        if (values.length == 0) {
            return targetClass.cast(null);
        }

        final Class<?> componentType = targetClass.getComponentType();
        final Object converted = Array.newInstance(componentType, values.length);
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i].isEmpty() ? null : convertValue(typeName, values[i]);
            Array.set(converted, i, componentType.cast(value));
        }

        return converted;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private Object convertValue(String typeName, String value) {
        assert !value.isEmpty() : "empty argument values should not be converted";
        switch (typeName) {
            case "Byte":
                return Byte.valueOf(value);
            case "Short":
                return Short.valueOf(value);
            case "Integer":
                return Integer.valueOf(value);
            case "Long":
                return Long.valueOf(value);
            case "Float":
                return Float.valueOf(value);
            case "Double":
                return Double.valueOf(value);
            case "String":
                return value;
            case "Character":
                return value.charAt(0);
            case "Boolean":
                return Boolean.valueOf(value);
            default:
                throw new HazelcastException("Unsupported PartitioningStrategy argument type: " + typeName);
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticComplexity", "checkstyle:ReturnCount"})
    private Class<?> convertTypeNameToClass(String typeName, boolean isArray) {
        switch (typeName) {
            case "Byte":
                return isArray ? Byte[].class : Byte.class;
            case "Short":
                return isArray ? Short[].class : Short.class;
            case "Integer":
                return isArray ? Integer[].class : Integer.class;
            case "Long":
                return isArray ? Long[].class : Long.class;
            case "Float":
                return isArray ? Float[].class : Float.class;
            case "Double":
                return isArray ? Double[].class : Double.class;
            case "String":
                return isArray ? String[].class : String.class;
            case "Character":
                return isArray ? Character[].class : Character.class;
            case "Boolean":
                return isArray ? Boolean[].class : Boolean.class;
            default:
                throw new HazelcastException("Unsupported PartitioningStrategy argument type: " + typeName);
        }
    }
}
