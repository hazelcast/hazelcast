package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.TestLoggingUtils;
import org.junit.Assert;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;


//@RunWith(HazelcastParallelClassRunner.class)
//@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultConfigValidationTest {
    private static final ILogger LOGGER;
    static int cnt = 0;

    static {
        TestLoggingUtils.initializeLogging();
        LOGGER = Logger.getLogger(DefaultConfigValidationTest.class);
    }

//    @Test
//    public void validateConfiguration() {
//        Tree configTree = new Tree();
//        for (Node configNode: configTree.getAllNodes()) {
//            Assert.assertEquals(configNode.defaultConfig, configNode.initialConfig);
//        }
//    }

    private static class Tree {
        Node root;

        Tree() {
            this.root = new Node(new Config(), null);
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

        Node(Object defaultConfig, Object initialConfig) {
            this(defaultConfig, initialConfig, new ArrayList<>());
        }

        Node(Object defaultConfig, Object initialConfig, List<Node> children) {
            this.defaultConfig = defaultConfig;
            this.initialConfig = initialConfig;
            this.children = children;
        }

        void populateChildren() {
            Class<?> clazz = defaultConfig.getClass();
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getName().startsWith("get") && method.getName().endsWith("Config")) {
                    try {
                        method.setAccessible(true);
                        Node child;
                        if (method.getParameterCount() == 0) {
                            Constructor<?> constructor = method.getReturnType().getDeclaredConstructor();
                            constructor.setAccessible(true);
                            child = new Node(constructor.newInstance(), method.invoke(this.defaultConfig));
                        } else {
                            Constructor<?> constructor = method.getReturnType().getDeclaredConstructor(String.class);
                            constructor.setAccessible(true);
                            child = new Node(constructor.newInstance("foo"), method.invoke(this.defaultConfig, "foo")); //TODO
                        }
                        if (child.initialConfig != null) {
                            boolean hasEquals = false;
                            for (Method m : method.getReturnType().getDeclaredMethods()) {
                                if (m.getName().equals("equals")) {
                                    children.add(child);
                                    hasEquals = true;
                                    cnt++;
                                    break;
                                }
                            }
                            if (!hasEquals) {
                                LOGGER.warning(method.getReturnType().getCanonicalName() + " (Child of " +
                                        defaultConfig.getClass().getCanonicalName() + ") should have implemented equals method.");

                            }
                        }
                    } catch (NoSuchMethodException e) {
                        LOGGER.warning(method.getReturnType().getCanonicalName() + " (Child of " +
                                defaultConfig.getClass().getCanonicalName() + ") does not have a suitable constructor.");
                    } catch (InstantiationException e) {
                        LOGGER.warning(
                                "Error occurred while calling the constructor of " + method.getReturnType().getCanonicalName() +
                                        " (Child of " + defaultConfig.getClass().getCanonicalName() + ")");
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

    public static void main(String[] args) {
        Tree t = new Tree();
        System.out.println(t.getAllNodes().size());
        System.out.println(cnt);
        int cnt2 = 0;
        for (Node configNode : t.getAllNodes()) {
            try {
                Assert.assertEquals(configNode.defaultConfig, configNode.initialConfig);
            } catch (Error e) {
                cnt2++;
            }
        }
        System.out.println(cnt2);
        Config config = new Config();
        System.out.println(config.getSplitBrainProtectionConfig("foo")); //TODO
        System.out.println(config.getWanReplicationConfig("foo"));
        System.out.println(config.getSecurityConfig().getRealmConfig("foo"));
        System.out.println(config.getServicesConfig().getServiceConfig("foo"));
    }
}
