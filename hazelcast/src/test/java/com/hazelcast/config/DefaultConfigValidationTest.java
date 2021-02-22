package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.TestLoggingUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultConfigValidationTest {
    private static final ILogger LOGGER;
    static int cnt = 0;

    static {
        TestLoggingUtils.initializeLogging();
        LOGGER = Logger.getLogger(DefaultConfigValidationTest.class);
    }

    private static class Tree {
        private final Map<Node, Method> allNodeMethodPairs = new HashMap<>();
        Node root;

        Tree() {
            Config config = new Config();
            this.root = new Node(config, allNodeMethodPairs);
            initialize();
        }

        void initialize() {
            root.populateChildren();
        }
    }

    private static class Node {
        private final List<Node> children;
        private final Object config;
        private final Map<Node, Method> allNodeMethodPairs;

        Node(Object config, Map<Node, Method> allNodeMethodPairs) {
            this(config, new ArrayList<>(), allNodeMethodPairs);
        }

        Node(Object config, List<Node> children, Map<Node, Method> allNodeMethodPairs) {
            this.config = config;
            this.children = children;
            this.allNodeMethodPairs = allNodeMethodPairs;
        }

        void populateChildren() {

            Class<?> clazz = config.getClass();
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getName().endsWith("Config") && method.getName().startsWith("get")) {
                    try {
                        Constructor<?> constructor = method.getReturnType().getDeclaredConstructor();
                        constructor.setAccessible(true);
                        Node child = new Node(constructor.newInstance(), this.allNodeMethodPairs);
                        children.add(child);
                        allNodeMethodPairs.put(child, method);
                        cnt++;
                    } catch (NoSuchMethodException e) {
                        LOGGER.warning(method.getReturnType().getCanonicalName() + " does not have a no-argument constructor.");
                    } catch (InstantiationException e) {
                        LOGGER.warning(
                                "Error occurred while calling the constructor of " + method.getReturnType().getCanonicalName());
                        LOGGER.warning(Arrays.toString(e.getStackTrace()));
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        // do not expect these types of exceptions
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

    public static void main(String[] args) {
        Tree t = new Tree();
        System.out.println(t.allNodeMethodPairs.size());
        System.out.println(cnt);
    }
}
