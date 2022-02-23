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

package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableSet;

/**
 * Holds blacklist and whitelist configuration in java deserialization configuration.
 */
public class ClassFilter {

    private static final String PROPERTY_CLASSNAME_LIMIT = "hazelcast.serialization.filter.classname.limit";
    private static final int CLASSNAME_LIMIT = Integer.getInteger(PROPERTY_CLASSNAME_LIMIT, 10000);
    private static final ILogger LOGGER = Logger.getLogger(ClassFilter.class);

    private final Set<String> classes;
    private final Set<String> packages;
    private final Set<String> prefixes;

    private AtomicBoolean warningLogged = new AtomicBoolean();

    public ClassFilter() {
        classes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        packages = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        prefixes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    }

    public ClassFilter(ClassFilter filter) {
        classes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        classes.addAll(filter.classes);
        packages = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        packages.addAll(filter.packages);
        prefixes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        prefixes.addAll(filter.prefixes);
        warningLogged = new AtomicBoolean(filter.warningLogged.get());
    }

    /**
     * Returns unmodifiable set of class names.
     */
    public Set<String> getClasses() {
        return unmodifiableSet(classes);
    }

    /**
     * Returns unmodifiable set of package names.
     */
    public Set<String> getPackages() {
        return unmodifiableSet(packages);
    }

    /**
     * Returns unmodifiable set of class name prefixes.
     */
    public Set<String> getPrefixes() {
        return unmodifiableSet(prefixes);
    }

    public ClassFilter addClasses(String... names) {
        checkNotNull(names);
        for (String name : names) {
            classes.add(name);
        }
        return this;
    }

    public ClassFilter setClasses(Collection<String> names) {
        checkNotNull(names);
        classes.clear();
        classes.addAll(names);
        return this;
    }

    public ClassFilter addPackages(String... names) {
        checkNotNull(names);
        for (String name : names) {
            packages.add(name);
        }
        return this;
    }

    public ClassFilter setPackages(Collection<String> names) {
        checkNotNull(names);
        packages.clear();
        packages.addAll(names);
        return this;
    }

    public ClassFilter addPrefixes(String... names) {
        checkNotNull(names);
        for (String name : names) {
            prefixes.add(name);
        }
        return this;
    }

    public ClassFilter setPrefixes(Collection<String> names) {
        checkNotNull(names);
        prefixes.clear();
        prefixes.addAll(names);
        return this;
    }

    public boolean isEmpty() {
        return classes.isEmpty() && packages.isEmpty() && prefixes.isEmpty();
    }

    public boolean isListed(String className) {
        if (classes.contains(className)) {
            return true;
        }
        if (!packages.isEmpty()) {
            int dotPosition = className.lastIndexOf(".");
            if (dotPosition > 0 && checkPackage(className, className.substring(0, dotPosition))) {
                return true;
            }
        }
        return checkPrefixes(className);
    }

    /**
     * Checks if given class name is listed by package. If it's listed, then performance optimization is used and classname is
     * added directly to {@code classes} collection.
     *
     * @param className   Class name to be checked.
     * @param packageName Package name of the checked class.
     * @return {@code true} iff class is listed by-package
     */
    private boolean checkPackage(String className, String packageName) {
        if (packages.contains(packageName)) {
            cacheClassname(className);
            return true;
        }
        return false;
    }

    private void cacheClassname(String className) {
        if (classes.size() < CLASSNAME_LIMIT) {
            // performance optimization
            classes.add(className);
        } else if (warningLogged.compareAndSet(false, true)) {
            LOGGER.warning(String.format(
                    "The class names collection size reached its limit. Optimizations for package names checks "
                            + "will not optimize next usages. You can control the class names collection size limit by "
                            + "setting system property '%s'. Actual value is %d.",
                    PROPERTY_CLASSNAME_LIMIT, CLASSNAME_LIMIT));
        }
    }

    private boolean checkPrefixes(String className) {
        for (String prefix : prefixes) {
            if (className.startsWith(prefix)) {
                cacheClassname(className);
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + classes.hashCode();
        result = prime * result + packages.hashCode();
        result = prime * result + prefixes.hashCode();
        result = prime * result + (warningLogged.get() ? 0 : 1);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClassFilter other = (ClassFilter) obj;
        return classes.equals(other.classes)
                && packages.equals(other.packages)
                && prefixes.equals(other.prefixes)
                && warningLogged.get() == other.warningLogged.get();
    }

    @Override
    public String toString() {
        return "ClassFilter{classes=" + classes + ", packages=" + packages + ", prefixes=" + prefixes + "}";
    }

}
