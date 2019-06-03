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

package com.hazelcast.internal.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.NamedConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;

/**
 * Utility class to access configuration.
 */
public final class ConfigUtils {

    private static final ILogger LOGGER = Logger.getLogger(Config.class);
    private static final BiConsumer<NamedConfig, String> DEFAULT_NAME_SETTER = new BiConsumer<NamedConfig, String>() {
        @Override
        public void accept(NamedConfig namedConfig, String name) {
            namedConfig.setName(name);
        }
    };

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
     * @throws ConfigurationException if ambiguous configurations are found
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
                Constructor constructor = clazz.getDeclaredConstructor();
                constructor.setAccessible(true);
                defConfig = (T) constructor.newInstance();
                nameSetter.accept(defConfig, "default");
                configs.put("default", defConfig);
            }
            Constructor copyConstructor = clazz.getDeclaredConstructor(clazz);
            copyConstructor.setAccessible(true);
            config = (T) copyConstructor.newInstance(defConfig);
            nameSetter.accept(config, name);
            configs.put(name, config);
            return config;
        } catch (NoSuchMethodException e) {
            LOGGER.severe("Could not create class " + clazz.getName());
            assert false;
            return null;
        } catch (InstantiationException e) {
            LOGGER.severe("Could not create class " + clazz.getName());
            assert false;
            return null;
        } catch (IllegalAccessException e) {
            LOGGER.severe("Could not create class " + clazz.getName());
            assert false;
            return null;
        } catch (InvocationTargetException e) {
            LOGGER.severe("Could not create class " + clazz.getName());
            assert false;
            return null;
        }
    }

}
