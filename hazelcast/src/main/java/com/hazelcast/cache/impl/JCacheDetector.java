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

package com.hazelcast.cache.impl;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.logging.ILogger;

import java.security.PrivilegedAction;

import static java.security.AccessController.doPrivileged;

/**
 * Utility class to detect existence of JCache 1.0.0 in the classpath.
 * Earlier versions (1.0.0-PFD, 0.9, 0.5) do not provide the complete JCache API. If a stale version
 * is detected, a warning will be logged and JCache is considered missing from the classpath.
 * <p>Related issues:
 * <ul>
 * <li>https://github.com/hazelcast/hazelcast/issues/7810</li>
 * <li>https://github.com/hazelcast/hazelcast/issues/7854</li>
 * </ul>
 */
public final class JCacheDetector {

    private static final String JCACHE_CACHING_CLASSNAME = "javax.cache.Caching";
    private static final String[] JCACHE_ADDITIONAL_REQUIRED_CLASSES = new String[]{
            "javax.cache.integration.CacheLoaderException",
            "javax.cache.integration.CacheWriterException",
            "javax.cache.processor.EntryProcessorException",
            "javax.cache.configuration.CompleteConfiguration",
    };

    // do not allow construction of instances
    private JCacheDetector() {
    }

    public static boolean isJCacheAvailable(ClassLoader classLoader) {
        return isJCacheAvailable((className) -> ClassLoaderUtil.isClassAvailable(classLoader, className));
    }

    /**
     * Only for testing
     */
    static boolean isJCacheAvailable(ClassAvailabilityChecker classAvailabilityChecker) {
        return isJCacheAvailable(null, classAvailabilityChecker);
    }

    /**
     * @param classLoader the class loader to use, when attempting to load JCache API classes.
     * @param logger      if not null and a pre-v1.0.0 JCache JAR is detected on the classpath,
     *                    logs a warning against using this version.
     * @return {@code true} if JCache 1.0.0 is located in the classpath, otherwise {@code false}.
     */
    public static boolean isJCacheAvailable(ClassLoader classLoader, ILogger logger) {
        return isJCacheAvailable(logger, (className) -> ClassLoaderUtil.isClassAvailable(classLoader, className));
    }

    /**
     * Only for testing
     */
    static boolean isJCacheAvailable(ILogger logger, ClassAvailabilityChecker classAvailabilityChecker) {
        ClassLoader backupClassLoader = doPrivileged((PrivilegedAction<ClassLoader>) JCacheDetector.class::getClassLoader);
        // try the class loader that loaded Hazelcast/JCacheDetector class
        // if the thread-context class loader is too narrowly defined and doesn't contain JCache
        return isJCacheAvailableInternal(logger, classAvailabilityChecker)
                || isJCacheAvailableInternal(logger, classAvailabilityChecker);
    }

    private static boolean isJCacheAvailableInternal(ILogger logger,
                                                     ClassAvailabilityChecker classAvailabilityChecker) {
        if (!classAvailabilityChecker.isClassAvailable(JCACHE_CACHING_CLASSNAME)) {
            // no cache-api jar in the classpath
            return false;
        }
        for (String className : JCACHE_ADDITIONAL_REQUIRED_CLASSES) {
            if (!classAvailabilityChecker.isClassAvailable(className)) {
                if (logger != null) {
                    logger.warning("An outdated version of JCache API was located in the classpath, please use newer versions of "
                            + "JCache API rather than 1.0.0-PFD or 0.x versions.");
                }
                return false;
            }
        }
        return true;
    }

    @FunctionalInterface
    interface ClassAvailabilityChecker {
        boolean isClassAvailable(String className);
    }

}
