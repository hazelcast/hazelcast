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

import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.partition.PartitioningStrategy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Factory for {@link PartitioningStrategy} instances.
 */
public final class PartitioningStrategyFactory {

    private static final String ARGUMENTS_REGEX = "(.*)\\((.+)\\)";
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
     * @param mapName Map for which this partitioning strategy is being created
     * @param config  the partitioning strategy configuration
     * @return
     */
    @SuppressWarnings("checkstyle:NestedIfDepth")
    public PartitioningStrategy getPartitioningStrategy(String mapName, PartitioningStrategyConfig config) {
        PartitioningStrategy strategy = null;
        if (config != null) {
            strategy = config.getPartitioningStrategy();
            if (strategy == null) {
                if (cache.containsKey(mapName)) {
                    strategy = cache.get(mapName);
                } else if (config.getPartitioningStrategyClass() != null) {
                    try {
                        if (classNameContainsArgs(config.getPartitioningStrategyClass())) {
                            strategy = constructPartitioningStrategyWithArgs(config.getPartitioningStrategyClass());
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

    private boolean classNameContainsArgs(String className) {
        return className.matches(ARGUMENTS_REGEX);
    }

    private PartitioningStrategy constructPartitioningStrategyWithArgs(
            final String classNameWithArgs
    ) throws ClassNotFoundException {
        final Matcher matcher = Pattern.compile(ARGUMENTS_REGEX).matcher(classNameWithArgs);
        if (!matcher.matches()) {
            throw new HazelcastException("Provided PartitionStrategy arguments are in invalid format: "
                    + classNameWithArgs);
        }

        assert matcher.groupCount() == 2;
        final String className = matcher.group(1);
        final String argsString = matcher.group(2);

        final String[] args = argsString.split(",");
        for (int i = 0; i < args.length; i++) {
            args[i] = args[i].trim();
        }

        final Class<?> strategyClass = ClassLoaderUtil.loadClass(configClassLoader, className);

        if (!PartitioningStrategy.class.isAssignableFrom(strategyClass)) {
            throw new HazelcastException("Provided PartitionStrategy class "
                    + "does not implement PartitionStrategy interface: " + strategyClass.getName());
        }

        final Constructor<?> constructor;
        try {
            constructor = strategyClass.getConstructor(String[].class);
        } catch (NoSuchMethodException e) {
            throw new HazelcastException("Could not find fitting constructor for specified PartitionStrategy: "
                    + classNameWithArgs, e);
        }

        try {
            return (PartitioningStrategy) constructor.newInstance(new Object[]{args});
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new HazelcastException("Failed to instantiate PartitionStrategy with constructor " + constructor, e);
        }
    }
}
