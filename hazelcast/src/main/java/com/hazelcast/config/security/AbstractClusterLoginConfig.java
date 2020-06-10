/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.security;

import java.util.Objects;
import java.util.Properties;

import javax.security.auth.Subject;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Abstract authentication configuration for types which configures login modules based on Hazelcast {@code ClusterLoginModule}
 *
 * @param <T> concrete authentication config type extending this one. It's used to return proper type in the fluent API setter
 *        methods.
 */
public abstract class AbstractClusterLoginConfig<T extends AbstractClusterLoginConfig<T>> implements AuthenticationConfig {

    private Boolean skipIdentity;
    private Boolean skipEndpoint;
    private Boolean skipRole;

    public Boolean getSkipIdentity() {
        return skipIdentity;
    }

    /**
     * If set to {@code true} then {@code ClusterIdentityPrincipal} instances are not added to JAAS {@link Subject} during
     * authentication.
     */
    public T setSkipIdentity(Boolean skipIdentity) {
        this.skipIdentity = skipIdentity;
        return self();
    }

    public Boolean getSkipEndpoint() {
        return skipEndpoint;
    }

    /**
     * If set to {@code true} then {@code ClusterEndpointPrincipal} instances are not added to JAAS {@link Subject} during
     * authentication.
     */
    public T setSkipEndpoint(Boolean skipEndpoint) {
        this.skipEndpoint = skipEndpoint;
        return self();
    }

    public Boolean getSkipRole() {
        return skipRole;
    }

    /**
     * If set to {@code true} then {@code ClusterRolePrincipal} instances are not added to JAAS {@link Subject} during
     * authentication.
     */
    public T setSkipRole(Boolean skipRole) {
        this.skipRole = skipRole;
        return self();
    }

    protected Properties initLoginModuleProperties() {
        Properties props = new Properties();
        setIfConfigured(props, "skipIdentity", skipIdentity);
        setIfConfigured(props, "skipEndpoint", skipEndpoint);
        setIfConfigured(props, "skipRole", skipRole);
        return props;
    }

    protected void setIfConfigured(Properties props, String propertyName, String value) {
        if (!isNullOrEmpty(value)) {
            props.setProperty(propertyName, value);
        }
    }

    protected void setIfConfigured(Properties props, String propertyName, Object value) {
        if (value != null) {
            props.setProperty(propertyName, value.toString());
        }
    }

    protected void setIfConfigured(Properties props, String propertyName, Enum<?> value) {
        if (value != null) {
            props.setProperty(propertyName, value.toString());
        }
    }

    protected abstract T self();

    @Override
    public int hashCode() {
        return Objects.hash(skipEndpoint, skipIdentity, skipRole);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AbstractClusterLoginConfig<?> other = (AbstractClusterLoginConfig<?>) obj;
        return Objects.equals(skipEndpoint, other.skipEndpoint) && Objects.equals(skipIdentity, other.skipIdentity)
                && Objects.equals(skipRole, other.skipRole);
    }
}
