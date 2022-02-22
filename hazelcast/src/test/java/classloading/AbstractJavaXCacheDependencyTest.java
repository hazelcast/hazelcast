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
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Method;
import java.util.List;

import static classloading.ThreadLocalLeakTestUtils.checkThreadLocalsForLeaks;
import static java.util.Collections.singletonList;

/**
 * Creates a member or client {@link com.hazelcast.core.Hazelcast} instance with an explicit exclusion of {@code javax.cache}.
 * <p>
 * If the method {@link #createHazelcastInstance()} or {@link #createHazelcastInstance_getCacheManager()} fails with a
 * {@link ClassNotFoundException} with the cause "javax.cache.* - Package excluded explicitly!" we accidentally introduced
 * a runtime dependency on {@link javax.cache} with a default configuration.
 * <p>
 * The method {@link #createHazelcastInstance_getCache()} is expected to fail, since it actually tries to invoke
 * {@link com.hazelcast.core.ICacheManager#getCache(String)}.
 */
public abstract class AbstractJavaXCacheDependencyTest {

    private static final String EXPECTED_CAUSE = "javax.cache.Cache - Package excluded explicitly!";

    private static final ClassLoader CLASS_LOADER;

    static {
        List<String> excludes = singletonList("javax.cache");
        CLASS_LOADER = new FilteringClassLoader(excludes, "com.hazelcast");
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void createHazelcastInstance() throws Exception {
        createHazelcastInstance(false, false);

        checkThreadLocalsForLeaks(CLASS_LOADER);
    }

    @Test
    public void createHazelcastInstance_getCacheManager() throws Exception {
        createHazelcastInstance(true, false);

        checkThreadLocalsForLeaks(CLASS_LOADER);
    }

    @Test
    public void createHazelcastInstance_getCache() throws Exception {
        createHazelcastInstance(true, true);

        checkThreadLocalsForLeaks(CLASS_LOADER);
    }

    protected abstract String getConfigClass();

    protected abstract String getHazelcastClass();

    protected abstract String getNewInstanceMethod();

    private void createHazelcastInstance(boolean testGetCacheManager, boolean testGetCache) throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        thread.setContextClassLoader(CLASS_LOADER);
        try {
            Class<?> configClazz = CLASS_LOADER.loadClass(getConfigClass());
            Class<?> hazelcastClazz = CLASS_LOADER.loadClass(getHazelcastClass());
            Class<?> hazelcastInstanceClazz = CLASS_LOADER.loadClass("com.hazelcast.core.HazelcastInstance");
            Class<?> cacheManagerClazz = CLASS_LOADER.loadClass("com.hazelcast.core.ICacheManager");
            try {
                Object config = configClazz.newInstance();

                Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);
                setClassLoader.invoke(config, CLASS_LOADER);

                Method newHazelcastInstance = hazelcastClazz.getDeclaredMethod(getNewInstanceMethod(), configClazz);
                Object hazelcastInstance = newHazelcastInstance.invoke(hazelcastClazz, config);

                if (testGetCacheManager) {
                    Method getCacheManager = hazelcastInstanceClazz.getDeclaredMethod("getCacheManager");
                    getCacheManager.invoke(hazelcastInstance);
                }
                if (testGetCache) {
                    expectedException.expect(new RootCauseMatcher(ClassNotFoundException.class, EXPECTED_CAUSE));
                    cacheManagerClazz.getDeclaredMethod("getCache", String.class);
                }
            } finally {
                Method shutdownAll = hazelcastClazz.getDeclaredMethod("shutdownAll");
                shutdownAll.invoke(hazelcastClazz);
            }
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }
}
