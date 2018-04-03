/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionPolicyConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.security.SecurityService;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * {@link SecurityConfig} wrapper that supports dynamically updating
 * client permissions.
 *
 * @see #setClientPermissionConfigs(Set)
 */
public class DynamicSecurityConfig extends SecurityConfig {

    private final SecurityConfig staticSecurityConfig;

    private final SecurityService securityService;

    public DynamicSecurityConfig(SecurityConfig staticSecurityConfig, SecurityService securityService) {
        this.staticSecurityConfig = staticSecurityConfig;
        this.securityService = securityService;
    }

    @Override
    public SecurityConfig addSecurityInterceptorConfig(SecurityInterceptorConfig interceptorConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public List<SecurityInterceptorConfig> getSecurityInterceptorConfigs() {
        return staticSecurityConfig.getSecurityInterceptorConfigs();
    }

    @Override
    public void setSecurityInterceptorConfigs(List<SecurityInterceptorConfig> securityInterceptorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public boolean isEnabled() {
        return staticSecurityConfig.isEnabled();
    }

    @Override
    public SecurityConfig setEnabled(boolean enabled) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SecurityConfig addMemberLoginModuleConfig(LoginModuleConfig loginModuleConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SecurityConfig addClientLoginModuleConfig(LoginModuleConfig loginModuleConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SecurityConfig addClientPermissionConfig(PermissionConfig permissionConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public List<LoginModuleConfig> getClientLoginModuleConfigs() {
        return staticSecurityConfig.getClientLoginModuleConfigs();
    }

    @Override
    public SecurityConfig setClientLoginModuleConfigs(List<LoginModuleConfig> loginModuleConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public List<LoginModuleConfig> getMemberLoginModuleConfigs() {
        return staticSecurityConfig.getMemberLoginModuleConfigs();
    }

    @Override
    public SecurityConfig setMemberLoginModuleConfigs(List<LoginModuleConfig> memberLoginModuleConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public PermissionPolicyConfig getClientPolicyConfig() {
        return staticSecurityConfig.getClientPolicyConfig();
    }

    @Override
    public SecurityConfig setClientPolicyConfig(PermissionPolicyConfig policyConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SecurityConfig setClientBlockUnmappedActions(boolean clientBlockUnmappedActions) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    /**
     * Returns existing client permissions as an unmodifiable {@link Set}. You may use this set to create your
     * client permissions set and pass it {@link #setClientPermissionConfigs(Set)} to update client permissions.
     */
    @Override
    public Set<PermissionConfig> getClientPermissionConfigs() {
        Set<PermissionConfig> permissionConfigs = securityService != null
                ? securityService.getClientPermissionConfigs()
                : staticSecurityConfig.getClientPermissionConfigs();
        return Collections.unmodifiableSet(permissionConfigs);
    }

    @Override
    public boolean getClientBlockUnmappedActions() {
        return staticSecurityConfig.getClientBlockUnmappedActions();
    }

    /**
     *  Updates client permission configuration cluster-wide.
     */
    @Override
    public SecurityConfig setClientPermissionConfigs(Set<PermissionConfig> permissions) {
        if (securityService == null) {
            throw new UnsupportedOperationException("Unsupported operation");
        }
        securityService.refreshClientPermissions(permissions);
        return this;
    }

    @Override
    public CredentialsFactoryConfig getMemberCredentialsConfig() {
        return staticSecurityConfig.getMemberCredentialsConfig();
    }

    @Override
    public SecurityConfig setMemberCredentialsConfig(CredentialsFactoryConfig credentialsFactoryConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        DynamicSecurityConfig that = (DynamicSecurityConfig) o;

        return staticSecurityConfig != null
                ? staticSecurityConfig.equals(that.staticSecurityConfig)
                : that.staticSecurityConfig == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (staticSecurityConfig != null ? staticSecurityConfig.hashCode() : 0);
        return result;
    }
}
