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

package com.hazelcast.spi.discovery;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * A common abstract superclass for {@link DiscoveryStrategy} implementations,
 * offering convenient access to configuration properties (which may be overridden
 * on the system's environment or JVM properties), as well as a {@link ILogger} instance.
 *
 * @since 3.6
 */
public abstract class AbstractDiscoveryStrategy implements DiscoveryStrategy {

    private final ILogger logger;
    private final Map<String, Comparable> properties;
    private final EnvVariableProvider envVariableProvider;

    public AbstractDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        this(logger, properties, System::getenv);
    }

    AbstractDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties, EnvVariableProvider envVariableProvider) {
        this.logger = logger;
        this.properties = unmodifiableMap(properties);
        this.envVariableProvider = envVariableProvider;
    }

    @Override
    public void destroy() {
    }

    @Override
    public void start() {
    }

    @Override
    public PartitionGroupStrategy getPartitionGroupStrategy() {
        return null;
    }

    @Override
    public PartitionGroupStrategy getPartitionGroupStrategy(Collection<? extends Member> allMembers) {
        return null;
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        return Collections.emptyMap();
    }

    /**
     * Returns an immutable copy of the configuration properties.
     *
     * @return the configuration properties
     */
    protected Map<String, Comparable> getProperties() {
        return properties;
    }

    /**
     * Returns a {@link ILogger} instance bound to the current class.
     *
     * @return a ILogger instance
     */
    protected ILogger getLogger() {
        return logger;
    }

    /**
     * Returns the value of the requested {@link PropertyDefinition} if available in the
     * declarative or programmatic configuration (XML or Config API), otherwise it will
     * return <code>null</code>.
     * <p>
     * <b>This method overload won't do environment or JVM property lookup.</b> A call to
     * this overload is equivalent to {@link #getOrNull(String, PropertyDefinition)}
     * with <code>null</code> passed as the first parameter.
     *
     * @param property the PropertyDefinition to lookup
     * @param <T>      the type of the property, must be compatible with the conversion
     *                 result of {@link PropertyDefinition#typeConverter()}
     * @return the value of the given property if available in the configuration, otherwise null
     */
    protected <T extends Comparable> T getOrNull(PropertyDefinition property) {
        return getOrDefault(property, null);
    }

    /**
     * Returns the value of the requested {@link PropertyDefinition} if available in the
     * declarative or programmatic configuration (XML or Config API), can be found in the
     * system's environment, or passed as a JVM property. Otherwise it will return <code>null</code>.
     * <p>
     * This overload will resolve the requested property in the following order, whereas the
     * higher priority is from top to bottom:
     * <ul>
     * <li>{@link System#getProperty(String)}: JVM properties</li>
     * <li>{@link System#getenv(String)}: System environment</li>
     * <li>Configuration properties of this {@link DiscoveryStrategy}</li>
     * </ul>
     * To resolve JVM properties or the system environment the property's key is prefixed with
     * given <code>prefix</code>, therefore a prefix of <i>com.hazelcast.discovery</i> and a property
     * key of <i>hostname</i> will result in a property lookup of <i>com.hazelcast.discovery.hostname</i>
     * in the system environment and JVM properties.
     *
     * @param prefix   the property key prefix for environment and JVM properties lookup
     * @param property the PropertyDefinition to lookup
     * @param <T>      the type of the property, must be compatible with the conversion
     *                 result of {@link PropertyDefinition#typeConverter()}
     * @return the value of the given property if available in the configuration, system environment
     * or JVM properties, otherwise null
     */
    protected <T extends Comparable> T getOrNull(String prefix, PropertyDefinition property) {
        return getOrDefault(prefix, property, null);
    }

    /**
     * Returns the value of the requested {@link PropertyDefinition} if available in the
     * declarative or programmatic configuration (XML or Config API), otherwise it will
     * return the given <code>defaultValue</code>.
     * <p>
     * <b>This method overload won't do environment or JVM property lookup.</b> A call to
     * this overload is equivalent to {@link #getOrDefault(String, PropertyDefinition, Comparable)}
     * with <code>null</code> passed as the first parameter.
     *
     * @param property the PropertyDefinition to lookup
     * @param <T>      the type of the property, must be compatible with the conversion
     *                 result of {@link PropertyDefinition#typeConverter()}
     * @return the value of the given property if available in the configuration, otherwise the
     * given default value
     */
    protected <T extends Comparable> T getOrDefault(PropertyDefinition property, T defaultValue) {
        return getOrDefault(null, property, defaultValue);
    }

    /**
     * Returns the value of the requested {@link PropertyDefinition} if available in the
     * declarative or programmatic configuration (XML or Config API), can be found in the
     * system's environment, or passed as a JVM property. otherwise it will return the given
     * <code>defaultValue</code>.
     * <p>
     * This overload will resolve the requested property in the following order, whereas the
     * higher priority is from top to bottom:
     * <ul>
     * <li>{@link System#getProperty(String)}: JVM properties</li>
     * <li>{@link System#getenv(String)}: System environment</li>
     * <li>Configuration properties of this {@link DiscoveryStrategy}</li>
     * </ul>
     * To resolve JVM properties or the system environment the property's key is prefixed with
     * given <code>prefix</code>, therefore a prefix of <i>com.hazelcast.discovery</i> and a property
     * key of <i>hostname</i> will result in a property lookup of <i>com.hazelcast.discovery.hostname</i>
     * in the system environment and JVM properties.
     *
     * @param prefix   the property key prefix for environment and JVM properties lookup
     * @param property the PropertyDefinition to lookup
     * @param <T>      the type of the property, must be compatible with the conversion
     *                 result of {@link PropertyDefinition#typeConverter()}
     * @return the value of the given property if available in the configuration, system environment
     * or JVM properties, otherwise the given default value
     */
    protected <T extends Comparable> T getOrDefault(String prefix, PropertyDefinition property, T defaultValue) {
        if (property == null) {
            return defaultValue;
        }

        Comparable value = readProperty(prefix, property);
        if (value == null) {
            value = properties.get(property.key());
        }

        if (value == null) {
            return defaultValue;
        }

        return (T) value;
    }

    private Comparable readProperty(String prefix, PropertyDefinition property) {
        if (prefix != null) {
            String p = getProperty(prefix, property);
            String v = System.getProperty(p);
            if (StringUtil.isNullOrEmpty(v)) {
                v = envVariableProvider.getVariable(p);
            }

            if (!StringUtil.isNullOrEmpty(v)) {
                return property.typeConverter().convert(v);
            }
        }
        return null;
    }

    private String getProperty(String prefix, PropertyDefinition property) {
        StringBuilder sb = new StringBuilder(prefix);
        if (prefix.charAt(prefix.length() - 1) != '.') {
            sb.append('.');
        }
        return sb.append(property.key()).toString();
    }

    @FunctionalInterface
    interface EnvVariableProvider {
        String getVariable(String name);
    }

}
