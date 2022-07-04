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

package com.hazelcast.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultConfigValidationTest extends HazelcastTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(DefaultConfigValidationTest.class);
    private static final HashMap<String, List<String>> IGNORED_CONFIGS = new HashMap<>();
    private static final String PREFIX = "com.hazelcast.config.";

    static {
        // By default MapStoreConfig should not be enabled when MapConfig is created. However when using MapStoreConfig
        // constructor it's logical to expect it's enabled by default.
        // EvictionConfig takes some default values (such as DEFAULT_MAX_SIZE) inside MapConfig.
        IGNORED_CONFIGS.put(PREFIX + "MapConfig", Arrays.asList(PREFIX + "EvictionConfig", PREFIX + "MapStoreConfig"));
        // Similar to MapStoreConfig
        IGNORED_CONFIGS.put(PREFIX + "RingbufferConfig", Collections.singletonList(PREFIX + "RingbufferStoreConfig"));
        // Default MergePolicy is HyperLogLogMergePolicy in CardinalityEstimatorConfig and PutIfAbsentMergePolicy in
        // MergePolicyConfig. MergePolicyConfig is a general-purpose configuration for handling split-brain healing and
        // HyperLogLogMergePolicy is the a specific policy for merging our HyperLogLog impl (the CardinalityEstimator)
        IGNORED_CONFIGS.put(PREFIX + "CardinalityEstimatorConfig", Collections.singletonList(PREFIX + "MergePolicyConfig"));
    }

    @Test
    public void validateServerConfiguration() throws Exception {
        ValidationTree t = new ValidationTree(new Config());
        int numberOfFailedConfigs = 0;

        for (ConfigNode configNode : t.getAllNodes()) {
            try {
                if (!(IGNORED_CONFIGS.containsKey(configNode.parent.name)
                        && IGNORED_CONFIGS.get(configNode.parent.name).contains(configNode.name))) {
                    assertEquals(configNode.defaultConfig, configNode.initialConfig);
                }
            } catch (Error e) {
                LOGGER.severe(configNode.name + " (Child of " + configNode.parent.name + ") failed the test.");
                numberOfFailedConfigs++;
            }
        }
        assertEquals(0, numberOfFailedConfigs);
    }

    @Test
    public void validateClientConfiguration() throws Exception {
        ValidationTree t = new ValidationTree(new ClientConfig());
        int numberOfFailedConfigs = 0;

        for (ConfigNode configNode : t.getAllNodes()) {
            try {
                assertEquals(configNode.defaultConfig, configNode.initialConfig);
            } catch (Error e) {
                LOGGER.severe(configNode.name + " (Child of " + configNode.parent.name + ") failed the test.");
                numberOfFailedConfigs++;
            }
        }
        assertEquals(0, numberOfFailedConfigs);
    }

    private static class ValidationTree {
        ConfigNode root;

        ValidationTree(Object config) throws Exception {
            this.root = new ConfigNode(config, null, null);
            root.populateChildren();
        }

        public List<ConfigNode> getAllNodes() {
            List<ConfigNode> nodes = new ArrayList<>();
            Queue<ConfigNode> queue = new ArrayDeque<>(root.children);
            while (!queue.isEmpty()) {
                ConfigNode curr = queue.poll();
                queue.addAll(curr.children);
                nodes.add(curr);
            }
            return nodes;
        }
    }

    private static class ConfigNode {
        private final Object defaultConfig; // Created with new XXXConfig()
        private final Object initialConfig; // Created with getXXXConfig()
        private final List<ConfigNode> children;
        private final ConfigNode parent; // Used for better logging
        private final String name; // Used for better logging

        ConfigNode(Object defaultConfig, Object initialConfig, ConfigNode parent) {
            this(defaultConfig, initialConfig, new ArrayList<>(), parent);
        }

        ConfigNode(Object defaultConfig, Object initialConfig, List<ConfigNode> children, ConfigNode parent) {
            this.defaultConfig = defaultConfig;
            this.initialConfig = initialConfig;
            this.children = children;
            this.parent = parent;
            this.name = defaultConfig.getClass().getName();
        }

        @SuppressWarnings("checkstyle:NestedIfDepth")
        void populateChildren() throws Exception {
            Class<?> clazz = defaultConfig.getClass();
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getName().startsWith("get") && method.getName().endsWith("Config")) {
                    try {
                        method.setAccessible(true);
                        ConfigNode child = null;
                        Object childInitialConfig;
                        if (method.getParameterCount() == 0) {
                            childInitialConfig = method.invoke(this.defaultConfig);
                            if (childInitialConfig == null) {
                                continue;
                            }
                            Constructor<?> constructor = method.getReturnType().getDeclaredConstructor();
                            constructor.setAccessible(true);
                            child = new ConfigNode(constructor.newInstance(), childInitialConfig, this);
                        } else if (method.getParameterCount() == 1) {
                            String randomString = randomString();
                            childInitialConfig = method.invoke(this.defaultConfig, randomString);
                            if (childInitialConfig == null) {
                                continue;
                            }
                            Constructor<?> constructor = method.getReturnType().getDeclaredConstructor(String.class);
                            constructor.setAccessible(true);
                            child = new ConfigNode(constructor.newInstance(randomString), childInitialConfig, this);
                        } else if (method.getParameterCount() == 2) {
                            if (method.getName().endsWith("getDeviceConfig")) {
                                childInitialConfig = method.invoke(this.defaultConfig, DeviceConfig.class, randomString());
                                if (childInitialConfig == null) {
                                    continue;
                                }
                                Constructor<?> constructor = childInitialConfig.getClass().getDeclaredConstructor();
                                constructor.setAccessible(true);
                                child = new ConfigNode(constructor.newInstance(), childInitialConfig, this);
                            } else {
                                // Only QueryCacheConfig enters here
                                String randomString1 = randomString(); // Map name
                                String randomString2 = randomString(); // Cache name
                                childInitialConfig = method.invoke(this.defaultConfig, randomString1, randomString2);
                                if (childInitialConfig == null) {
                                    continue;
                                }
                                Constructor<?> constructor = method.getReturnType().getDeclaredConstructor(String.class);
                                constructor.setAccessible(true);
                                child = new ConfigNode(constructor.newInstance(randomString2), childInitialConfig, this);
                            }
                        } else {
                            LOGGER.warning(method.getReturnType().getCanonicalName()
                                    + " (Child of " + name + ") is called from " + method.getName()
                                    + ". We don't handle more than 2 parameter getters");
                        }
                        if (child != null && child.initialConfig != null) {
                            boolean hasEquals = false;
                            for (Method m : method.getReturnType().getMethods()) {
                                if (m.getName().equals("equals") && !m.getDeclaringClass().equals(Object.class)) {
                                    children.add(child);
                                    hasEquals = true;
                                    break;
                                }
                            }
                            if (!hasEquals) {
                                LOGGER.warning(method.getReturnType().getCanonicalName()
                                        + " (Child of " + name + ") should have implemented equals method.");

                            }
                        }
                    } catch (NoSuchMethodException e) {
                        LOGGER.warning(method.getReturnType().getCanonicalName()
                                + " (Child of " + name + ") does not have a suitable constructor.");
                        throw e;
                    } catch (InstantiationException e) {
                        if (!(method.getReturnType() == SecureStoreConfig.class)) {
                            LOGGER.warning("Error occurred while calling the constructor of "
                                    + method.getReturnType().getCanonicalName() + " (Child of " + name + ")");
                            throw e;
                        }
                    }
                }
            }
            populateGrandChildren();
        }

        void populateGrandChildren() throws Exception {
            for (ConfigNode child : children) {
                child.populateChildren();
            }
        }
    }
}
