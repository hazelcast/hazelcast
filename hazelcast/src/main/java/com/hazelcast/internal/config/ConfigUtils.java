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

package com.hazelcast.internal.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NamedConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;
import static java.lang.String.format;

/**
 * Utility class to access configuration.
 */
public final class ConfigUtils {

    private static final ILogger LOGGER = Logger.getLogger(Config.class);
    private static final BiConsumer<NamedConfig, String> DEFAULT_NAME_SETTER = NamedConfig::setName;

    private ConfigUtils() {
    }

    public static <T> T lookupByPattern(ConfigPatternMatcher configPatternMatcher, Map<String, T> configPatterns,
                                        String itemName) {
        T candidate = configPatterns.get(itemName);
        if (candidate != null) {
            return candidate;
        }
        String configPatternKey = configPatternMatcher.matches(configPatterns.keySet(), itemName);
        if (configPatternKey != null) {
            return configPatterns.get(configPatternKey);
        }
        if (!"default".equals(itemName) && !itemName.startsWith("hz:")) {
            LOGGER.finest("No configuration found for " + itemName + ", using default config!");
        }
        return null;
    }

    /**
     * Returns a config for the given name, creating one
     * if necessary and adding it to the collection of known configurations.
     * <p>
     * The configuration is found by matching the configuration name
     * pattern to the provided {@code name} without the partition qualifier
     * (the part of the name after {@code '@'}).
     * If no configuration matches, it will create one by cloning the
     * {@code "default"} configuration and add it to the configuration
     * collection.
     * <p>
     * This method is intended to easily and fluently create and add
     * configurations more specific than the default configuration without
     * explicitly adding it by invoking addXConfig(..)
     * <p>
     * Because it adds new configurations if they are not already present,
     * this method is intended to be used before this config is used to
     * create a hazelcast instance. Afterwards, newly added configurations
     * may be ignored.
     *
     * @param name name of the config
     * @return the configuration
     * @throws com.hazelcast.config.InvalidConfigurationException if ambiguous configurations are found
     * @see StringPartitioningStrategy#getBaseName(java.lang.String)
     * @see Config#setConfigPatternMatcher(ConfigPatternMatcher)
     * @see Config#getConfigPatternMatcher()
     */
    public static <T extends NamedConfig> T getConfig(ConfigPatternMatcher configPatternMatcher,
                                                      Map<String, T> configs, String name,
                                                      Class clazz) {
        return getConfig(configPatternMatcher, configs, name, clazz, (BiConsumer<T, String>) DEFAULT_NAME_SETTER);
    }

    /**
     * Similar to {@link ConfigUtils#getConfig(ConfigPatternMatcher, Map, String, Class)}
     * This method is introduced specifically for solving problem of EventJournalConfig
     * use {@link ConfigUtils#getConfig(ConfigPatternMatcher, Map, String, Class)} along with {@link NamedConfig}
     * where possible
     */
    public static <T> T getConfig(ConfigPatternMatcher configPatternMatcher,
                                  Map<String, T> configs, String name,
                                  Class clazz, BiConsumer<T, String> nameSetter) {
        name = getBaseName(name);
        T config = lookupByPattern(configPatternMatcher, configs, name);
        if (config != null) {
            return config;
        }
        T defConfig = configs.get("default");
        try {
            if (defConfig == null) {
                defConfig = (T) constructReflectively(clazz);
                nameSetter.accept(defConfig, "default");
                configs.put("default", defConfig);
            }
            Constructor copyConstructor = clazz.getDeclaredConstructor(clazz);
            copyConstructor.setAccessible(true);
            config = (T) copyConstructor.newInstance(defConfig);
            nameSetter.accept(config, name);
            configs.put(name, config);
            return config;
        } catch (NoSuchMethodException | InstantiationException
                | IllegalAccessException | InvocationTargetException e) {
            LOGGER.severe("Could not create class " + clazz.getName());
            assert false;
            return null;
        }
    }

    /**
     * If {@code configs} contains an exact match for {@code name}, returns
     * the matching config. Otherwise creates a new config with the given
     * name, adds it to {@code configs} and returns it.
     */
    public static <T extends NamedConfig, S extends T> T getByNameOrNew(Map<String, T> configs, String name,
                                      Class<S> clazz) {
        T config = configs.get(name);
        if (config != null) {
            return config;
        } else {
            try {
                config = constructReflectively(clazz);
                config.setName(name);
                configs.put(name, config);
                return config;
            } catch (NoSuchMethodException | InstantiationException
                    | IllegalAccessException | InvocationTargetException e) {
                LOGGER.severe("Could not create class " + clazz.getName());
                assert false;
                return null;
            }
        }
    }

    @Nonnull
    private static <T> T constructReflectively(Class<T> clazz)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        T config;
        Constructor constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        config = (T) constructor.newInstance();
        return config;
    }

    public static InvalidConfigurationException createAmbiguousConfigurationException(
      String itemName, String candidate, String duplicate) {
        return new InvalidConfigurationException(
          format("Found ambiguous configurations for item\"%s\": \"%s\" vs. \"%s\"%n"
            + "Please specify your configuration.", itemName, candidate, duplicate));
    }

    public static boolean matches(String configName, String configName2) {
        return configName != null
          && configName2 != null
          && configName.replace("-", "").equals(configName2.replace("-", ""));
    }
}

