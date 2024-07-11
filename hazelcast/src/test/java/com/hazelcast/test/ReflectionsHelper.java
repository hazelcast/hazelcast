/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import org.reflections.Reflections;
import org.reflections.Store;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.QueryFunction;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collection;
import java.util.function.Predicate;

import static org.reflections.scanners.Scanners.SubTypes;

/**
 * Initialize the org.reflections library once to avoid duplicate work on scanning the classpath from individual tests.
 */
public final class ReflectionsHelper {

    public static final Reflections REFLECTIONS;

    /**
     * Excludes classes that does not belong to {@code com.hazelcast} package or
     * belongs to {@code com.hazelcast.internal.json} package.
     */
    public static final Predicate<String> EXCLUDE_NON_HAZELCAST_CLASSES =
            c -> c.startsWith("com.hazelcast") && !c.startsWith("com.hazelcast.internal.json");

    /**
     * Excludes enums, interfaces, abstract and anonymous classes.
     */
    public static final Predicate<Class<?>> EXCLUDE_NON_CONCRETE_CLASSES =
            c -> !(Enum.class.isAssignableFrom(c) || c.isAnonymousClass()
                    || c.isInterface() || Modifier.isAbstract(c.getModifiers()));

    static {
        // Obtain all classpath URLs with com.hazelcast package classes.
        Collection<URL> comHazelcastPackageURLs = ClasspathHelper.forPackage("com.hazelcast");
        // Exclude hazelcast test artifacts (hazelcast-VERSION-tests.jar, **/target/test-classes/)
        // and hazelcast-license-extractor artifact from package URLs.
        comHazelcastPackageURLs.removeIf(url -> url.toString().contains("-tests.jar")
                || url.toString().contains("target/test-classes")
                || url.toString().contains("hazelcast-license-extractor"));

        REFLECTIONS = new Reflections(new ConfigurationBuilder().addUrls(comHazelcastPackageURLs));
    }

    private ReflectionsHelper() { }

    /**
     * Excludes {@linkplain #EXCLUDE_NON_HAZELCAST_CLASSES non-Hazelcast classes} from the results.
     */
    @SuppressWarnings({"ClassGetClass", "RedundantCast", "unchecked"})
    public static <T> QueryFunction<Store, Class<? extends T>> subTypesOf(Class<T> type) {
        return SubTypes.of(type)
                .filter(EXCLUDE_NON_HAZELCAST_CLASSES)
                .as((Class<Class<? extends T>>) type.getClass());
    }

    /**
     * Excludes {@linkplain #EXCLUDE_NON_HAZELCAST_CLASSES non-Hazelcast classes} and
     * {@linkplain #EXCLUDE_NON_CONCRETE_CLASSES non-concrete classes} from the results.
     */
    public static <T> QueryFunction<Store, Class<? extends T>> concreteSubTypesOf(Class<T> type) {
        return subTypesOf(type)
                .filter(EXCLUDE_NON_CONCRETE_CLASSES);
    }
}
