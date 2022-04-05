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

package classloading;

import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.lang.reflect.Method;
import java.util.Collections;

import static classloading.ThreadLocalLeakTestUtils.checkThreadLocalsForLeaks;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

/**
 * Checks Hazelcast for {@link ThreadLocal} leaks after the shutdown.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ThreadLocalLeakTest {

    private enum ClassLoaderType {
        FILTERING,
        OWN,
    }

    @Parameter
    public ClassLoaderType classLoaderType;

    private Class<?> applicationClazz;

    @Parameters(name = "classLoaderType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{
                {ClassLoaderType.FILTERING},
                //{ClassLoaderType.OWN},
        });
    }

    @After
    public void tearDown() {
        if (applicationClazz != null) {
            cleanupLeakingApplication(applicationClazz);
        }
    }

    /**
     * Checks the detection code with an example application which uses {@link ThreadLocal} with a proper cleanup.
     */
    @Test
    public void testLeakingApplication_withThreadLocalCleanup() throws Exception {
        ClassLoader cl = getClassLoader(LeakingApplication.class.getPackage().getName());

        applicationClazz = startLeakingApplication(cl, true);

        checkThreadLocalsForLeaks(cl);
    }

    /**
     * Checks the detection code with an example application which leaks {@link ThreadLocal} instances.
     */
    @Test(expected = AssertionError.class)
    public void testLeakingApplication_withoutThreadLocalCleanup() throws Exception {
        ClassLoader cl = getClassLoader(LeakingApplication.class.getPackage().getName());

        applicationClazz = startLeakingApplication(cl, false);

        checkThreadLocalsForLeaks(cl);
    }

    /**
     * Tests Hazelcast for {@link ThreadLocal} leakages.
     */
    @Test
    public void testHazelcast() throws Exception {
        ClassLoader cl = getClassLoader("com.hazelcast");

        Object isolatedNode = startIsolatedNode(cl);
        assertTrue(getClusterTime(isolatedNode) > 0);

        shutdownIsolatedNode(isolatedNode);
        checkThreadLocalsForLeaks(cl);
    }

    private ClassLoader getClassLoader(String packageName) {
        switch (classLoaderType) {
            case FILTERING:
                return new FilteringClassLoader(Collections.<String>emptyList(), packageName);
            case OWN:
                return getClass().getClassLoader();
            default:
                throw new AssertionError("Unknown classLoaderType: " + classLoaderType);
        }
    }

    private static Class<?> startLeakingApplication(ClassLoader cl, boolean doCleanup) {
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(cl);

            Class<?> applicationClazz = cl.loadClass(LeakingApplication.class.getCanonicalName());
            Method init = applicationClazz.getDeclaredMethod("init", Boolean.class);
            init.invoke(applicationClazz, doCleanup);

            return applicationClazz;
        } catch (Exception e) {
            throw new RuntimeException("Could not start LeakingApplication", e);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    private static void cleanupLeakingApplication(Class<?> applicationClazz) {
        try {
            Method cleanup = applicationClazz.getDeclaredMethod("cleanup");
            cleanup.invoke(applicationClazz);
        } catch (Exception e) {
            throw new RuntimeException("Could not cleanup LeakingApplication", e);
        }
    }

    private static Object startIsolatedNode(ClassLoader cl) {
        Object isolatedNode;
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(cl);

            Class<?> configClazz = cl.loadClass("com.hazelcast.config.Config");
            Object config = configClazz.newInstance();
            Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);
            setClassLoader.invoke(config, cl);

            Class<?> hazelcastClazz = cl.loadClass("com.hazelcast.core.Hazelcast");
            Method newHazelcastInstance = hazelcastClazz.getDeclaredMethod("newHazelcastInstance", configClazz);
            isolatedNode = newHazelcastInstance.invoke(hazelcastClazz, config);
        } catch (Exception e) {
            throw new RuntimeException("Could not start isolated Hazelcast instance", e);
        } finally {
            thread.setContextClassLoader(tccl);
        }
        return isolatedNode;
    }

    private static void shutdownIsolatedNode(Object isolatedNode) {
        try {
            Class<?> instanceClass = isolatedNode.getClass();
            Method method = instanceClass.getMethod("shutdown");
            method.invoke(isolatedNode);
        } catch (Exception e) {
            throw new RuntimeException("Could not start shutdown Hazelcast instance", e);
        }
    }

    private static long getClusterTime(Object isolatedNode) {
        try {
            Method getCluster = isolatedNode.getClass().getMethod("getCluster");
            Object cluster = getCluster.invoke(isolatedNode);
            Method getClusterTime = cluster.getClass().getMethod("getClusterTime");
            return ((Number) getClusterTime.invoke(cluster)).longValue();
        } catch (Exception e) {
            throw new RuntimeException("Could not get cluster time from Hazelcast instance", e);
        }
    }
}
