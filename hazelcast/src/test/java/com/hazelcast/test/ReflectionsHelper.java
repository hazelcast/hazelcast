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

package com.hazelcast.test;

import com.google.common.collect.Sets;
import org.reflections.Configuration;
import org.reflections.ReflectionUtils;
import org.reflections.Reflections;
import org.reflections.adapters.JavaReflectionAdapter;
import org.reflections.scanners.AbstractScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

/**
 * Initialize the org.reflections library once to avoid duplicate work on scanning the classpath from individual tests.
 */
@SuppressWarnings("WeakerAccess")
public final class ReflectionsHelper {

    public static final Reflections REFLECTIONS;

    static {
        // obtain all classpath URLs with com.hazelcast package classes
        Collection<URL> comHazelcastPackageURLs = ClasspathHelper.forPackage("com.hazelcast");
        // exclude hazelcast test artifacts from package URLs
        for (Iterator<URL> iterator = comHazelcastPackageURLs.iterator(); iterator.hasNext(); ) {
            URL url = iterator.next();
            // detect hazelcast-VERSION-tests.jar & $SOMEPATH/hazelcast/target/test-classes/ and exclude it from classpath
            // also exclude hazelcast-license-extractor artifact
            if (url.toString().contains("-tests.jar") || url.toString().contains("target/test-classes")
                    || url.toString().contains("hazelcast-license-extractor")) {
                iterator.remove();
            }
        }
        HierarchyTraversingSubtypesScanner subtypesScanner = new HierarchyTraversingSubtypesScanner();
        subtypesScanner.setResultFilter(new FilterBuilder().exclude("java\\.lang\\.(Object|Enum)")
                .exclude("com\\.hazelcast\\.internal\\.json.*"));
        REFLECTIONS = new ReflectionsTransitive(new ConfigurationBuilder().addUrls(comHazelcastPackageURLs)
                .addScanners(subtypesScanner, new TypeAnnotationsScanner())
                .setMetadataAdapter(new JavaReflectionAdapter()));
    }

    private ReflectionsHelper() {
    }

    /**
     * Removes abstract and anonymous classes and interfaces from the given set.
     */
    public static void filterNonConcreteClasses(Set<? extends Class> classes) {
        classes.removeIf(klass -> klass.isAnonymousClass()
                || klass.isInterface() || Modifier.isAbstract(klass.getModifiers())
        );
    }

    /**
     * Removes the classes that does not belong to `com.hazelcast` package.
     */
    public static void filterNonHazelcastClasses(Set<? extends Class> classes) {
        classes.removeIf(klass -> !klass.getName().startsWith("com.hazelcast"));
    }

    /**
     * Overrides the default implementation of {@link Reflections#getSubTypesOf(Class)}
     * to also obtain transitive subtypes of the given class.
     */
    public static class ReflectionsTransitive extends Reflections {

        public ReflectionsTransitive(Configuration configuration) {
            super(configuration);
        }

        @Override
        public <T> Set<Class<? extends T>> getSubTypesOf(Class<T> type) {
            return Sets.newHashSet(ReflectionUtils.<T>forNames(
                    store.getAll(
                            HierarchyTraversingSubtypesScanner.class.getSimpleName(),
                            singletonList(type.getName())),
                    configuration.getClassLoaders()));
        }
    }

    public static class HierarchyTraversingSubtypesScanner extends AbstractScanner {

        /**
         * Creates new HierarchyTraversingSubtypesScanner.
         * <p>
         * Excludes direct Object subtypes.
         */
        public HierarchyTraversingSubtypesScanner() {
            // exclude direct Object subtypes by default
            this(true);
        }

        /**
         * Creates a new HierarchyTraversingSubtypesScanner.
         *
         * @param excludeObjectClass excludes direct {@link Object} subtypes in results if {@code true}
         */
        public HierarchyTraversingSubtypesScanner(boolean excludeObjectClass) {
            if (excludeObjectClass) {
                // exclude direct Object subtypes
                filterResultsBy(new FilterBuilder().exclude(Object.class.getName()));
            }
        }

        /**
         * @param cls depending on the Reflections configuration, this is either a regular Class or a javassist ClassFile
         */
        @SuppressWarnings("unchecked")
        public void scan(final Object cls) {
            String className = getMetadataAdapter().getClassName(cls);

            for (String anInterface : (List<String>) getMetadataAdapter().getInterfacesNames(cls)) {
                if (acceptResult(anInterface)) {
                    getStore().put(anInterface, className);
                }
            }

            // apart from this class' direct supertype and directly declared interfaces, also scan the class
            // hierarchy up until Object class
            Class superKlass = ((Class) cls).getSuperclass();
            while (superKlass != null) {
                scanClassAndInterfaces(superKlass, className);
                superKlass = superKlass.getSuperclass();
            }
        }

        @SuppressWarnings({"unchecked"})
        private void scanClassAndInterfaces(Class klass, String className) {
            if (acceptResult(klass.getName())) {
                getStore().put(klass.getName(), className);
                for (String anInterface : (List<String>) getMetadataAdapter().getInterfacesNames(klass)) {
                    if (acceptResult(anInterface)) {
                        getStore().put(anInterface, className);
                    }
                }
            }
        }
    }
}
