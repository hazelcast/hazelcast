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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.internal.namespace.NamespaceService;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.SerializationClassNameFilter;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.jet.impl.util.ReflectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static com.hazelcast.config.ConfigAccessor.getResourceDefinitions;
import static com.hazelcast.jet.impl.JobRepository.classKeyName;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;

/**
 * Actual implementation for {@link NamespaceService} used when Namespaces are enabled.
 * Replaced by {@link NoOpNamespaceService} when Namespaces are disabled.
 */
public final class NamespaceServiceImpl implements NamespaceService {

    /** Map of available {@link MapResourceClassLoader} instances, keyed by Namespace name*/
    private final ConcurrentMap<String, MapResourceClassLoader> namespaceToClassLoader = new ConcurrentHashMap<>();

    /** The fallback {@link ClassLoader} to use for awareness */
    private final ClassLoader configClassLoader;
    /** Config-populated filter for Namespace loaded classes */
    private final SerializationClassNameFilter classFilter;
    private boolean hasDefaultNamespace;

    public NamespaceServiceImpl(ClassLoader configClassLoader, Map<String, NamespaceConfig> nsConfigs,
                                JavaSerializationFilterConfig filterConfig) {
        this.configClassLoader = configClassLoader;
        if (filterConfig != null) {
            this.classFilter = new SerializationClassNameFilter(filterConfig);
        } else {
            this.classFilter = null;
        }
        nsConfigs.forEach((nsName, nsConfig) -> addNamespace(nsName, getResourceDefinitions(nsConfig)));
    }

    /**
     * @inheritDocs
     */
    @Override
    public boolean isEnabled() {
        return true;
    }

    /**
     * @inheritDocs
     */
    @Override
    public boolean isDefaultNamespaceDefined() {
        return hasDefaultNamespace;
    }

    /**
     * @inheritDocs
     */
    @Override
    public void addNamespace(@Nonnull String nsName, @Nonnull Collection<ResourceDefinition> resources) {
        Objects.requireNonNull(nsName, "namespace name cannot be null");
        Objects.requireNonNull(resources, "resources cannot be null");

        Map<String, byte[]> resourceMap = new ConcurrentHashMap<>();
        for (ResourceDefinition r : resources) {
            handleResource(r, resourceMap);
        }

        MapResourceClassLoader updated = new MapResourceClassLoader(nsName, configClassLoader, () -> resourceMap, true);

        MapResourceClassLoader removed = namespaceToClassLoader.put(nsName, updated);
        if (removed != null) {
            cleanUpClassLoader(nsName, removed);
        }
        initializeClassLoader(nsName, updated);
        if (nsName.equals(DEFAULT_NAMESPACE_NAME)) {
            hasDefaultNamespace = true;
        }
    }

    /**
     * @inheritDocs
     */
    @Override
    public boolean removeNamespace(@Nonnull String nsName) {
        MapResourceClassLoader removed = namespaceToClassLoader.remove(nsName);
        if (removed != null) {
            cleanUpClassLoader(nsName, removed);
            if (nsName.equals(DEFAULT_NAMESPACE_NAME)) {
                hasDefaultNamespace = false;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * @inheritDocs
     */
    @Override
    public boolean hasNamespace(String namespace) {
        return namespaceToClassLoader.containsKey(namespace);
    }

    // Namespace setup/cleanup handling functions

    private void setupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }

        ClassLoader loader = getClassLoaderForExactNamespace(namespace);
        if (loader == null) {
            throw new IllegalArgumentException(String.format("There is no environment defined for provided Namespace: %s\n"
                    + "Add a new NamespaceConfig with the name '%s' and define resources to use this Namespace.",
                    namespace, namespace));
        }

        NamespaceThreadLocalContext.onStartNsAware(loader);
    }

    private static void cleanupNs(@Nullable String namespace) {
        if (namespace == null) {
            return;
        }
        NamespaceThreadLocalContext.onCompleteNsAware(namespace);
    }

    /**
     * @inheritDocs
     */
    @Override
    public void setupNamespace(@Nullable String namespace) {
        setupNs(transformNamespace(namespace));
    }

    /**
     * @inheritDocs
     */
    @Override
    public void cleanupNamespace(@Nullable String namespace) {
        cleanupNs(transformNamespace(namespace));
    }

    /**
     * @inheritDocs
     */
    @Override
    public void runWithNamespace(@Nullable String namespace, Runnable runnable) {
        namespace = transformNamespace(namespace);
        setupNs(namespace);
        try {
            runnable.run();
        } finally {
            cleanupNs(namespace);
        }
    }

    /**
     * @inheritDocs
     */
    @Override
    public <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        namespace = transformNamespace(namespace);
        setupNs(namespace);
        try {
            return callable.call();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        } finally {
            cleanupNs(namespace);
        }
    }

    /**
     * @inheritDocs
     */
    @Override
    public ClassLoader getClassLoaderForNamespace(@Nullable String namespace) {
        namespace = transformNamespace(namespace);
        if (namespace != null) {
            return namespaceToClassLoader.get(namespace);
        }
        return null;
    }

    // Internal method to transform a `null` namespace into the default namespace if available
    private String transformNamespace(String namespace) {
        if (namespace != null) {
            return namespace;
            // Check if we have a `default` environment available
        } else if (isDefaultNamespaceDefined()) {
            return DEFAULT_NAMESPACE_NAME;
        } else {
            // Namespace is null, no default Namespace is defined, fail-fast
            return null;
        }
    }

    // Resource/classloader handling functions

    private void handleResource(ResourceDefinition resource, Map<String, byte[]> resourceMap) {
        switch (resource.type()) {
            case JAR:
                 handleJar(resource.id(), resource.payload(), resourceMap);
                break;
            case JARS_IN_ZIP:
                handleJarInZip(resource.id(), resource.payload(), resourceMap);
                break;
            case CLASS:
                handleClass(resource.id(), resource.payload(), resourceMap);
                break;
            default:
                throw new IllegalArgumentException("Cannot handle resource type " + resource.type());
        }
    }

    /**
     * Add classes and files in the given {@code jarBytes} to the provided {@code resourceMap}, after appropriate
     * encoding:
     * <ul>
     *     <li>Payload is deflated</li>
     *     <li>For each JAR entry in {@code jarBytes} that is a class, its class name is converted to a resource ID with the
     *     {@code "c."} prefix followed by the class name converted to a path.</li>
     *     <li>For other JAR entries in {@code jarBytes}, its path is converted to a resource ID with the
     *     {@code "f."} prefix followed by the path.</li>
     * </ul>
     * <p>
     * Caller is responsible for closing stream.
     * @param id
     * @param inputStream
     * @param resourceMap
     * @see     com.hazelcast.jet.impl.util.ReflectionUtils#toClassResourceId(String)
     * @see     JobRepository#classKeyName(String)
     * @see     JobRepository#fileKeyName(String)
     */
    private void handleJar(String id, InputStream inputStream, Map<String, byte[]> resourceMap) {
        try {
            JarInputStream jarInputStream = new JarInputStream(inputStream);

            JarEntry entry;
            do {
                entry = jarInputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }
                String className = ClassLoaderUtil.extractClassName(entry.getName());
                if (classFilter != null) {
                    classFilter.filter(className);
                }
                byte[] payload = IOUtil.compress(jarInputStream.readAllBytes());
                jarInputStream.closeEntry();
                resourceMap.put(className == null ? JobRepository.fileKeyName(entry.getName())
                        : JobRepository.classKeyName(toClassResourceId(className)), payload);
            } while (true);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read from JAR bytes for resource with id " + id, e);
        }
    }

    private void handleJarInZip(String id, byte[] zipBytes, Map<String, byte[]> resourceMap) {
        try (InputStream inputStream = new ByteArrayInputStream(zipBytes)) {
            JobRepository.executeOnJarsInZIP(inputStream, zis -> handleJar(id, zis, resourceMap));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read from JAR bytes for resource with id " + id, e);
        }
    }

    /**
     * Add the class to the {@code resourceMap}, extracting and using the class' binary name (package and class) as the
     * {@code resourceMap} key, after performing deflate compression on its payload.
     * @param id the resource ID, used for reference in exceptions
     * @param classBytes the class binary content
     * @param resourceMap resource map to add resource to
     * @see com.hazelcast.jet.impl.util.ReflectionUtils#toClassResourceId
     * @see com.hazelcast.jet.impl.util.ReflectionUtils#getInternalBinaryName
     */
    private void handleClass(String id, byte[] classBytes, Map<String, byte[]> resourceMap) {
        try {
            // Extract a fully qualified class name - ID can be user customized, and URL can be anything
            String fqClassName = ReflectionUtils.getInternalBinaryName(classBytes);
            if (classFilter != null) {
                classFilter.filter(fqClassName);
            }
            // We need to append `.class` as per ReflectionUtils#toClassResourceId, but we don't need the path replacement
            resourceMap.put(classKeyName(fqClassName + ".class"), IOUtil.compress(classBytes));
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to read from CLASS bytes for resource with id " + id, ex);
        }
    }

    /** @see #handleJar(String, InputStream, Map<String, byte[]>) */
    private void handleJar(String id, byte[] jarBytes, Map<String, byte[]> resourceMap) {
        try (InputStream stream = new ByteArrayInputStream(jarBytes)) {
            handleJar(id, stream, resourceMap);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read from JAR bytes for resource with id " + id, e);
        }
    }

    private static void initializeClassLoader(String nsName, MapResourceClassLoader classLoader) {
        NamespaceAwareDriverManagerInterface.initializeJdbcDrivers(nsName, classLoader);
    }

    private static void cleanUpClassLoader(String nsName, MapResourceClassLoader removedClassLoader) {
        NamespaceAwareDriverManagerInterface.cleanupJdbcDrivers(nsName, removedClassLoader);
    }

    MapResourceClassLoader getClassLoaderForExactNamespace(@Nonnull String namespace) {
        return namespaceToClassLoader.get(namespace);
    }
}
