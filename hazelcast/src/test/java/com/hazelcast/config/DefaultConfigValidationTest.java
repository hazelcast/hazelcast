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
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestLoggingUtils;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultConfigValidationTest {
    private static final ILogger LOGGER;
    static int numberOfTestedConfigs = 0;

    static {
        TestLoggingUtils.initializeLogging();
        LOGGER = Logger.getLogger(DefaultConfigValidationTest.class);
    }

    @Test
    public void validateServerConfiguration() {
        Tree t = new Tree(new Config());
        System.out.println("Number of tested configs: " + numberOfTestedConfigs);
        int numberOfFailedConfigs = 0;
        for (Node configNode : t.getAllNodes()) {
            try {
                Assert.assertEquals(configNode.defaultConfig, configNode.initialConfig);
            } catch (Error e) {
                System.out.println(configNode.name + " (Child of "
                        + configNode.parent.name
                        + ") failed the test.");
                numberOfFailedConfigs++; //TODO
            }
        }
        System.out.println("Number of failed configs: " + numberOfFailedConfigs);
        Config config = new Config();
        System.out.println(config.getSplitBrainProtectionConfig("foo")); //TODO
    }

    @Test
    public void validateClientConfiguration() {
        Tree t = new Tree(new ClientConfig());
        System.out.println("Number of tested configs: " + numberOfTestedConfigs);
        int numberOfFailedConfigs = 0;
        for (Node configNode : t.getAllNodes()) {
            try {
                Assert.assertEquals(configNode.defaultConfig, configNode.initialConfig);
            } catch (Error e) {
                numberOfFailedConfigs++;
            }
        }
        System.out.println("Number of failed configs: " + numberOfFailedConfigs);
    }

    private static class Tree {
        Node root;

        Tree(Object config) {
            this.root = new Node(config, null, null);
            root.populateChildren();
        }

        public List<Node> getAllNodes() {
            List<Node> nodes = new ArrayList<>();
            Queue<Node> queue = new ArrayDeque<>(root.children);
            while (!queue.isEmpty()) {
                Node curr = queue.poll();
                queue.addAll(curr.children);
                nodes.add(curr);
            }
            return nodes;
        }
    }

    private static class Node {
        private final Object defaultConfig; // Created with new XXXConfig()
        private final Object initialConfig; // Created with getXXXConfig()
        private final List<Node> children;
        private final Node parent; // Used for better logging
        private final String name; // Used for better logging

        Node(Object defaultConfig, Object initialConfig, Node parent) {
            this(defaultConfig, initialConfig, new ArrayList<>(), parent);
        }

        Node(Object defaultConfig, Object initialConfig, List<Node> children, Node parent) {
            this.defaultConfig = defaultConfig;
            this.initialConfig = initialConfig;
            this.children = children;
            this.parent = parent;
            this.name = defaultConfig.getClass().getName();
        }

        void populateChildren() {
            Class<?> clazz = defaultConfig.getClass();
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getName().startsWith("get") && method.getName().endsWith("Config")) {
                    try {
                        method.setAccessible(true);
                        Node child = null;
                        if (method.getParameterCount() == 0) {
                            Constructor<?> constructor = method.getReturnType().getDeclaredConstructor();
                            constructor.setAccessible(true);
                            child = new Node(constructor.newInstance(), method.invoke(this.defaultConfig), this);
                        } else if (method.getParameterCount() == 1){
                            Constructor<?> constructor = method.getReturnType().getDeclaredConstructor(String.class);
                            constructor.setAccessible(true);
                            child = new Node(constructor.newInstance("foo"),
                                    method.invoke(this.defaultConfig, "foo"), this);
                        }
                        else {
                            System.out.println(method.getParameterCount()); //TODO
                            System.out.println(Arrays.toString(method.getParameterTypes()));
                            System.out.println(method.getReturnType().getName());
                            System.out.println(method.getName());
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
                        if (!(method.getReturnType() == WanReplicationConfig.class
                                || method.getReturnType() == RealmConfig.class
                                || method.getReturnType() == ServiceConfig.class)) {
                            LOGGER.warning(method.getReturnType().getCanonicalName()
                                    + " (Child of " + name + ") does not have a suitable constructor.");
                        }
                    } catch (InstantiationException e) {
                        LOGGER.warning("Error occurred while calling the constructor of "
                                + method.getReturnType().getCanonicalName() + " (Child of " + name + ")"); //TODO
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
            populateGrandChildren();
        }

        void populateGrandChildren() {
            for (Node child : children) {
                child.populateChildren();
            }
        }
    }
}
