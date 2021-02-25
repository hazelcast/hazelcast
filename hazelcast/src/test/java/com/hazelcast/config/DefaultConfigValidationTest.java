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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultConfigValidationTest extends HazelcastTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(DefaultConfigValidationTest.class);
    static int numberOfTestedConfigs = 0;

    @Test
    public void validateServerConfiguration()
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        ValidationTree t = new ValidationTree(new Config());
        int numberOfFailedConfigs = 0;
        LOGGER.info("Number of tested configs: " + numberOfTestedConfigs);

        for (ConfigNode configNode : t.getAllNodes()) {
            try {
                assertEquals(configNode.defaultConfig, configNode.initialConfig);
            } catch (Error e) {
                LOGGER.warning(configNode.name + " (Child of " + configNode.parent.name + ") failed the test.");
                numberOfFailedConfigs++;
            }
        }
        LOGGER.info("Number of failed configs: " + numberOfFailedConfigs);
        assertEquals(0, numberOfFailedConfigs);
    }

    @Test
    public void validateClientConfiguration()
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        ValidationTree t = new ValidationTree(new ClientConfig());
        int numberOfFailedConfigs = 0;
        LOGGER.info("Number of tested configs: " + numberOfTestedConfigs);

        for (ConfigNode configNode : t.getAllNodes()) {
            try {
                assertEquals(configNode.defaultConfig, configNode.initialConfig);
            } catch (Error e) {
                LOGGER.warning(configNode.name + " (Child of " + configNode.parent.name + ") failed the test.");
                numberOfFailedConfigs++;
            }
        }
        LOGGER.info("Number of failed configs: " + numberOfFailedConfigs);
        assertEquals(0, numberOfFailedConfigs);
    }

    private static class ValidationTree {
        ConfigNode root;

        ValidationTree(Object config)
                throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
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

        void populateChildren()
                throws InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
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
                                    numberOfTestedConfigs++;
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
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
            }
            populateGrandChildren();
        }

        void populateGrandChildren()
                throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
            for (ConfigNode child : children) {
                child.populateChildren();
            }
        }
    }
}
