package com.hazelcast.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class DefaultConfigValidationTest {
    static int cnt = 0;

    private static class ConfigTree {
        Node root;

        ConfigTree(Node root) {
            this.root = root;
        }
    }

    private static class Node {
        Object config;
        Node parent;
        Map<Node, Method> children;

        Node(Object config) {
            this(config, null, new HashMap<>());
        }

        Node(Object config, Node parent) {
            this(config, parent, new HashMap<>());
        }

        Node(Object config, Node parent, Map<Node, Method> children) {
            this.config = config;
            this.parent = parent;
            this.children = children;
        }

        void populateChildren() {

            Class<?> clazz = config.getClass();
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getName().endsWith("Config") && method.getName().startsWith("get")) {
                    try {
                        Constructor<?> constructor = method.getReturnType().getDeclaredConstructor();
                        constructor.setAccessible(true);
                        Node child = new Node(constructor.newInstance(), this);
                        children.put(child, method);
                        cnt++;
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                        System.out.println(method.getName());
                        e.printStackTrace(); //TODO
                    }
                }
            }
            populateGrandChildren();
        }

        void populateGrandChildren() {

            for (Node child : children.keySet()) {
                child.populateChildren();
            }
        }
    }

    public static void main(String[] args) {

        Config config = new Config();
        Node node = new Node(config);
        node.populateChildren();
        System.out.println(cnt);


    }
}
