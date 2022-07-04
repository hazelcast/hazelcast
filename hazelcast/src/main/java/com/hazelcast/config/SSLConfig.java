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

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * SSL configuration.
 */
public final class SSLConfig extends AbstractFactoryWithPropertiesConfig<SSLConfig> {

    private Object factoryImplementation;

    public SSLConfig() {
    }

    public SSLConfig(SSLConfig sslConfig) {
        factoryImplementation = sslConfig.factoryImplementation;
        setEnabled(sslConfig.isEnabled());
        factoryClassName = sslConfig.getFactoryClassName();
        Properties properties = new Properties();
        properties.putAll(sslConfig.getProperties());
        setProperties(properties);
    }

    /**
     * Sets the name for the implementation class.
     * <p>
     * Class can either be an {@link com.hazelcast.nio.ssl.SSLContextFactory} or {@code com.hazelcast.nio.ssl.SSLEngineFactory}
     * (Enterprise edition).
     *
     * @param factoryClassName the name implementation class
     */
    public SSLConfig setFactoryClassName(@Nonnull String factoryClassName) {
        super.setFactoryClassName(factoryClassName);
        this.factoryImplementation = null;
        return this;
    }

    /**
     * Sets the implementation object.
     * <p>
     * Object must be instance of an {@link com.hazelcast.nio.ssl.SSLContextFactory} or
     * {@code com.hazelcast.nio.ssl.SSLEngineFactory} (Enterprise edition).
     *
     * @param factoryImplementation the implementation object
     * @return this SSLConfig instance
     */
    public SSLConfig setFactoryImplementation(@Nonnull Object factoryImplementation) {
        this.factoryImplementation = checkNotNull(factoryImplementation, "SSL context factory cannot be null!");
        factoryClassName = null;
        return this;
    }

    /**
     * Returns the factory implementation object.
     * <p>
     * Object is instance of an {@link com.hazelcast.nio.ssl.SSLContextFactory} or {@code com.hazelcast.nio.ssl.SSLEngineFactory}
     * (Enterprise edition).
     *
     * @return the factory implementation object
     */
    public Object getFactoryImplementation() {
        return factoryImplementation;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(factoryImplementation);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SSLConfig other = (SSLConfig) obj;
        return Objects.equals(factoryImplementation, other.factoryImplementation);
    }

    @Override
    public String toString() {
        return "SSLConfig [factoryImplementation=" + factoryImplementation + ", getFactoryClassName()=" + getFactoryClassName()
                + ", isEnabled()=" + isEnabled() + ", getProperties()=" + getProperties() + "]";
    }

    @Override
    protected SSLConfig self() {
        return this;
    }

}
