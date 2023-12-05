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

package com.hazelcast.internal.namespace;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.dynamicconfig.DynamicConfigYamlGenerator;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NamespaceTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.hazelcast.internal.namespace.UCDTest.AssertionStyle.NEGATIVE;
import static com.hazelcast.internal.namespace.UCDTest.AssertionStyle.POSITIVE;
import static com.hazelcast.internal.namespace.UCDTest.ClassRegistrationStyle.INSTANCE_IN_CONFIG;
import static com.hazelcast.internal.namespace.UCDTest.ClassRegistrationStyle.INSTANCE_IN_DATA_STRUCTURE;
import static com.hazelcast.internal.namespace.UCDTest.ClassRegistrationStyle.NONE;
import static com.hazelcast.internal.namespace.UCDTest.ConfigStyle.DYNAMIC;
import static com.hazelcast.internal.namespace.UCDTest.ConfigStyle.STATIC_PROGRAMMATIC;
import static com.hazelcast.internal.namespace.UCDTest.ConfigStyle.STATIC_XML;
import static com.hazelcast.internal.namespace.UCDTest.ConfigStyle.STATIC_YAML;
import static com.hazelcast.internal.namespace.UCDTest.ConnectionStyle.CLIENT_TO_MEMBER;
import static com.hazelcast.internal.namespace.UCDTest.ConnectionStyle.EMBEDDED;
import static com.hazelcast.internal.namespace.UCDTest.ConnectionStyle.MEMBER_TO_MEMBER;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @see <a href="https://hazelcast.atlassian.net/browse/HZ-3597">HZ-3597 - Add unit tests for all @NamespacesSupported UDF
 *      interfaces, across all supported data structures</a>
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({SlowTest.class, NamespaceTest.class})
public abstract class UCDTest extends HazelcastTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(UCDTest.class);
    private static final Path sourceJar = Paths.get("src/test/class", "usercodedeployment", "UCDTest.jar");
    private ClassLoader resourceClassLoader;

    @Parameter(0)
    public ConnectionStyle connectionStyle;
    @Parameter(1)
    public ConfigStyle configStyle;
    @Parameter(2)
    public ClassRegistrationStyle classRegistrationStyle;
    @Parameter(3)
    public AssertionStyle assertionStyle;

    private TestHazelcastFactory testHazelcastFactory;

    protected HazelcastInstance member;
    protected HazelcastInstance instance;

    private NamespaceConfig namespaceConfig;

    protected String objectName = randomName();

    @Before
    public void setUp() {
        testHazelcastFactory = new TestHazelcastFactory();
    }

    @After
    public void tearDown() {
        testHazelcastFactory.shutdownAll();
    }

    /** Don't annotate children with {@code @Before}, framework controls test execution */
    private void setUpInstance() throws Exception {
        initialiseConfig();

        Config config = smallInstanceConfigWithoutJetAndMetrics();

        namespaceConfig = new NamespaceConfig(getNamespaceName());
        namespaceConfig.addJar(sourceJar.toUri().toURL(), "ucd_test_jar");

        config.getNamespacesConfig().setEnabled(assertionStyle == POSITIVE);

        if (configStyle != ConfigStyle.DYNAMIC) {
            setupConfigs(config);
            config = convertConfig(config);
        }

        member = testHazelcastFactory.newHazelcastInstance(config);

        switch (connectionStyle) {
            case EMBEDDED:
                instance = member;
                break;
            case CLIENT_TO_MEMBER:
                instance = testHazelcastFactory.newHazelcastClient();
                break;
            case MEMBER_TO_MEMBER:
                instance = testHazelcastFactory.newHazelcastInstance(config.setLiteMember(true));
                break;
            default:
                throw new IllegalArgumentException(connectionStyle.toString());
        }

        // If you want to apply configuration on the data structure, obviously you need to initialize it first
        if (classRegistrationStyle == ClassRegistrationStyle.INSTANCE_IN_DATA_STRUCTURE) {
            initialiseDataStructure();
        }

        if (configStyle == ConfigStyle.DYNAMIC) {
            setupConfigs(instance.getConfig());
        }

        // But if you want to apply configuration via config, then you need to do this before you initialize it
        if (classRegistrationStyle != ClassRegistrationStyle.INSTANCE_IN_DATA_STRUCTURE) {
            initialiseDataStructure();
        }
    }

    private void setupConfigs(Config config) {
        registerNamespacesConfig(config);
        classRegistrationStyle.action.accept(this);
        registerConfig(config);
    }

    /**
     * A place to construct an instance of {@link MapConfig} or similar
     * <p>
     * Don't annotate children with {@code @Before}, framework controls test execution
     */
    protected abstract void initialiseConfig();

    /**
     * A place to construct an instance of {@link IMap} or similar
     * <p>
     * Don't annotate children with {@code @Before}, framework controls test execution
     */
    protected abstract void initialiseDataStructure() throws Exception;

    /**
     * Where applicable, transforms our programmatic config into an XML/YAML variant and feeds it back into a new {@link Config}
     * instance, thereby validating XML/YAML parsing.
     *
     * @param config the programmatic config to convert
     * @return the newly converted {@link Config}
     */
    private Config convertConfig(Config config) {
        String configString;
        switch (configStyle) {
            case STATIC_XML:
                configString = new ConfigXmlGenerator(true, false).generate(config);
                break;

            case STATIC_YAML:
                configString = new DynamicConfigYamlGenerator().generate(config, false);
                break;

            default:
                // No conversion necessary
                return config;
        }
        return Config.loadFromString(configString);
    }

    /**
     * Executes {@link #test()}, and checking it's result against the expected {@link #assertionStyle}
     * <p>
     * It's possible (and neater) to implement this as a pair of {@link Test}s and use
     * {@code assumeTrue(assertionStyle=AssertionStyle.XYZ)} to switch between execution implementation at runtime, but then you
     * have twice the {@link Before} overhead when half is never used.
     */
    @Test
    public void executeTest() throws Exception {
        try {
            setUpInstance();
            test();
        } catch (Throwable t) {
            switch (assertionStyle) {
                case NEGATIVE:
                    // Print the exception for tracing (useful to have), but it's expected, so return
                    t.printStackTrace();
                    return;
                case POSITIVE:
                    throw t;
                default:
                    throw new IllegalArgumentException(assertionStyle.toString());
            }
        }

        assertEquals("Test passed even though namespace was not configured, suggests scope of test is incorrect",
                POSITIVE, assertionStyle);
    }

    /** Don't annotate children with {@code @Test}, framework controls test execution */
    public abstract void test() throws Exception;

    /**
     * Classes to be loaded into the environment within the scope of this test
     * <p>
     * Typically main class under test is first
     */
    protected abstract String getUserDefinedClassName();

    /** Allows registration of a config (e.g. a {@link MapConfig} with the {@link HazelcastInstance#getConfig()} */
    protected abstract void registerConfig(Config config);

    /**
     * Allows registration of a class with the configuration of the data structure, based on an instance of the class
     * <p>
     * E.G. {@link EntryListenerConfig#setImplementation(MapListener)}
     * <p>
     * Implementers should *consider* overriding this behavior.
     *
     * @throws ReflectiveOperationException because implementers will be constructing a class, likely using reflection
     * @throws UnsupportedOperationException if the test does not support this kind of configuration
     *
     * @see ClassRegistrationStyle#INSTANCE_IN_CONFIG
     */
    protected void addClassInstanceToConfig() throws ReflectiveOperationException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Allows registration of a class with the configuration of the data structure, based on the name of the class
     * <p>
     * E.G. {@link ListenerConfig#setClassName(String)}
     * <p>
     * Implementers should *consider* overriding this behavior.
     *
     * @throws UnsupportedOperationException if the test does not support this kind of configuration
     *
     * @see ClassRegistrationStyle#NAME_IN_CONFIG
     */
    protected void addClassNameToConfig() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Allows registration of a class with the data structure itself, based on an instance of the class
     * <p>
     * E.G. {@link IMap#addEntryListener(MapListener, boolean)}
     * <p>
     * Implementers should *consider* overriding this behavior.
     *
     * @throws ReflectiveOperationException because implementers will be constructing a class, likely using reflection
     * @throws UnsupportedOperationException if the test does not support this kind of configuration
     *
     * @see ClassRegistrationStyle#INSTANCE_IN_DATA_STRUCTURE
     */
    protected void addClassInstanceToDataStructure() throws ReflectiveOperationException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    private void registerNamespacesConfig(Config config) {
        config.getNamespacesConfig().addNamespaceConfig(namespaceConfig);
    }

    /** @return an instance of the {@link Class} returned from {@link #getClassObject()} */
    protected <T> T getClassInstance() throws ReflectiveOperationException {
        return (T) getClassObject().getDeclaredConstructor().newInstance();
    }

    private void initResourceClassLoader() {
        if (resourceClassLoader != null) {
            return;
        }
        // We want to use a MapResourceClassLoader that's relevant for the cluster, as using an
        //  externally created instance from our test JAR can result in our `instance` using that
        //  externally loaded resource and passing all NEGATIVE assertions because the instance
        //  never actually goes through serde (client listeners, for example)
        NodeEngine engine;
        switch (connectionStyle) {
            case CLIENT_TO_MEMBER:
                engine = getNodeEngineImpl(member);
                break;
            case MEMBER_TO_MEMBER:
            case EMBEDDED:
                engine = getNodeEngineImpl(instance);
                break;
            default: throw new IllegalArgumentException();
        }
        resourceClassLoader = NamespaceUtil.getClassLoaderForNamespace(engine, getNamespaceName());
    }

    /** @return the {@link Class} object generated from {@link #getUserDefinedClassName()} */
    private Class<?> getClassObject() throws ReflectiveOperationException {
        // Init our ClassLoader at call time to ensure our backing instance is ready
        initResourceClassLoader();
        Class<?> clazz = resourceClassLoader.loadClass(getUserDefinedClassName());
        Class<?> unwanted = IdentifiedDataSerializable.class;
        assertFalse(String.format(
                "%s should not implement %s, as unless done with care, when deserialized the parent might be deserialized instead",
                getUserDefinedClassName(), unwanted.getSimpleName()), unwanted.isAssignableFrom(clazz));
        return clazz;
    }

    protected String getNamespaceName() {
        return "ns1";
    }

    /**
     * We expect that listeners implement {@code usercodedeployment.ObservableListener}
     * <p>
     * When a listeners' event is fired, it calls {@code usercodedeployment.ObservableListener.record(Object)} which:
     * <ol>
     * <li>Creates an {@link IMap} with the name of the listener (derived, via reflection, from the caller) - e.g.
     * {@code MyEntryListener}
     * <li>Puts an entry in the map where:
     * <ul>
     * <li>Key = The name of the method in the listener (derived, via reflection, from the caller) - e.g. {@code entryAdded}
     * <li>Value = {@link Object#toString()} of the event - this is because we cannot ensure the event is
     * {@link java.io.Serializable} in it's native form
     * </ul>
     * </ol>
     * <p>
     * To check for this firing, we can {@link #assertTrueEventually(AssertTask)} a corresponding entry exists.
     * <p>
     * If this was documented in the class itself, it would get lost during compilation
     *
     * @param key the name of the method in the listener that should've fired
     */
    protected void assertListenerFired(String key) throws ReflectiveOperationException {
        Collection<String> result = instance.getSet(getClassObject().getSimpleName());

        assertTrueEventually(() -> {
            if (LOGGER.isFinestEnabled()) {
                LOGGER.finest("Checking map for values, currently contains %s", result);
            }

            assertTrue(result.contains(key));
        });
    }

    protected enum ConnectionStyle {
        /** Work directly with the underlying {@link HazelcastInstance} */
        EMBEDDED,
        /** Test communication between {@link HazelcastClient} & member */
        CLIENT_TO_MEMBER,
        /** Test communication between members - using a lite member as the entry point */
        MEMBER_TO_MEMBER;
    }

    protected enum ConfigStyle {
        /** All configuration is set programmatically <strong>before</strong> the instance is started */
        STATIC_PROGRAMMATIC,
        /** Where possible, configuration is changed <strong>after</strong> the instance has started */
        DYNAMIC,
        /** All configuration is defined by a YAML configuration, derived from our programmatic config */
        STATIC_YAML,
        /** All configuration is defined by an XML configuration, derived from our programmatic config */
        STATIC_XML;
    }

    /**
     * Classes can be registered in multiple ways, parameterized to allow tests to define what they support and then each to be
     * tested the same way
     * <p>
     * called methods are expected to be inherited and extended so must call a method in the enclosing class.
     */
    protected enum ClassRegistrationStyle {
        INSTANCE_IN_CONFIG(instance -> {
            try {
                instance.addClassInstanceToConfig();
            } catch (ReflectiveOperationException e) {
                throw ExceptionUtil.sneakyThrow(e);
            }
        }),
        NAME_IN_CONFIG(UCDTest::addClassNameToConfig),
        INSTANCE_IN_DATA_STRUCTURE(instance -> {
             try {
                 instance.addClassInstanceToDataStructure();
             } catch (ReflectiveOperationException e) {
                 throw ExceptionUtil.sneakyThrow(e);
             }
        }),
        NONE(instance -> { });

        private final Consumer<UCDTest> action;

        ClassRegistrationStyle(Consumer<UCDTest> action) {
            this.action = action;
        }
    }

    public enum AssertionStyle {
        /** Happy path - assert the functionality works when configured correctly */
        POSITIVE,
        /**
         * Negative path - assert that the functionality doesn't work normally when namespace not configured to ensure scope of
         * test is correct
         */
        NEGATIVE;
    }

    @Parameters(name = "Connection: {0}, Config: {1}, Class Registration: {2}, Assertion: {3}")
    public static Iterable<Object[]> parameters() {
        // We don't need a `NEGATIVE` assertion for all tests; we can validate this by handling
        //   `NEGATIVE` assertions for a handful of tests
        // We don't need all ClassRegistrationStyle values, most tests will use `NONE` and those
        //   that use the other values can override parameters to provide all but `NONE`
        // We *probably* don't need all config styles for all 3 connection styles, but to avoid
        //   accidentally missing use cases, they will be kept
        return List.of(
                // Client to member
                new Object[]{CLIENT_TO_MEMBER, STATIC_PROGRAMMATIC, NONE, POSITIVE},
                new Object[]{CLIENT_TO_MEMBER, STATIC_XML, NONE, POSITIVE},
                new Object[]{CLIENT_TO_MEMBER, STATIC_YAML, NONE, POSITIVE},
                new Object[]{CLIENT_TO_MEMBER, DYNAMIC, NONE, POSITIVE},
                new Object[]{CLIENT_TO_MEMBER, STATIC_PROGRAMMATIC, NONE, NEGATIVE},
                new Object[]{CLIENT_TO_MEMBER, DYNAMIC, NONE, NEGATIVE},

                // Member to member
                new Object[]{MEMBER_TO_MEMBER, STATIC_PROGRAMMATIC, NONE, POSITIVE},
                new Object[]{MEMBER_TO_MEMBER, STATIC_XML, NONE, POSITIVE},
                new Object[]{MEMBER_TO_MEMBER, STATIC_YAML, NONE, POSITIVE},
                new Object[]{MEMBER_TO_MEMBER, DYNAMIC, NONE, POSITIVE},
                new Object[]{MEMBER_TO_MEMBER, STATIC_PROGRAMMATIC, NONE, NEGATIVE},
                new Object[]{MEMBER_TO_MEMBER, DYNAMIC, NONE, NEGATIVE},

                // Embedded
                new Object[]{EMBEDDED, STATIC_PROGRAMMATIC, NONE, POSITIVE},
                new Object[]{EMBEDDED, STATIC_XML, NONE, POSITIVE},
                new Object[]{EMBEDDED, STATIC_YAML, NONE, POSITIVE},
                new Object[]{EMBEDDED, DYNAMIC, NONE, POSITIVE},
                new Object[]{EMBEDDED, STATIC_XML, NONE, NEGATIVE},
                new Object[]{EMBEDDED, DYNAMIC, NONE, NEGATIVE}
        );
    }

    // Used for tests where we need different ClassRegistrationStyle approaches (and not used in
    //   all other tests to reduce complexity of parameterized testing)
    protected static List<Object[]> listenerParameters() {
        List<Object[]> newParams = new ArrayList<>(45);
        for (Object[] parameter : parameters()) {
            // Create variants for all "not NONE" ClassRegistrationStyles
            for (ClassRegistrationStyle style : ClassRegistrationStyle.values()) {
                if (style != NONE) {
                    // Only run INSTANCE_IN_DATA_STRUCTURE tests on embedded/member-to-member (clients invoke
                    //  listeners locally), and avoid negative assertions due to classpath implications
                    if (style == INSTANCE_IN_DATA_STRUCTURE
                            && (parameter[0] == CLIENT_TO_MEMBER || parameter[1] != DYNAMIC || parameter[3] == NEGATIVE)) {
                        continue;
                    }
                    // Do not include NEGATIVE assertions for INSTANCE_IN_CONFIG tests on embedded/member-to-member
                    //  since there is nothing to validate due to classpath implications
                    if (style == INSTANCE_IN_CONFIG
                            && (parameter[1] != DYNAMIC || (parameter[0] != CLIENT_TO_MEMBER && parameter[3] == NEGATIVE))) {
                        // Only applicable to DYNAMIC ConfigStyles on CLIENT_TO_MEMBER ConnectionStyles because
                        //   when running member to member or embedded, we need the class to be loaded for us
                        //   to obtain an instance to actually add anyway - so there's nothing to validate
                        continue;
                    }
                    newParams.add(new Object[]{parameter[0], parameter[1], style, parameter[3]});
                }
            }
            // We don't want `NONE` variants as tests using these parameters require class registration
        }
        return newParams;
    }

    // For fast access in tests that do not support INSTANCE_IN_DATA_STRUCTURE
    protected static List<Object[]> listenerParametersWithoutInstanceInDataStructure() {
        return listenerParameters().stream()
                                   .filter(obj -> obj[2] != ClassRegistrationStyle.INSTANCE_IN_DATA_STRUCTURE)
                                   .collect(Collectors.toList());
    }
}
