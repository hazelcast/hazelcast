/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Contains configuration for Security
 */
public class SecurityConfig {

    private static final boolean DEFAULT_CLIENT_BLOCK_UNMAPPED_ACTIONS = true;

    private boolean enabled;

    private CredentialsFactoryConfig memberCredentialsConfig = new CredentialsFactoryConfig();

    private List<LoginModuleConfig> memberLoginModuleConfigs = new ArrayList<LoginModuleConfig>();

    private List<SecurityInterceptorConfig> securityInterceptorConfigs = new ArrayList<SecurityInterceptorConfig>();

    private List<LoginModuleConfig> clientLoginModuleConfigs = new ArrayList<LoginModuleConfig>();

    private PermissionPolicyConfig clientPolicyConfig = new PermissionPolicyConfig();

    private Set<PermissionConfig> clientPermissionConfigs = new HashSet<PermissionConfig>();

    private boolean clientBlockUnmappedActions = DEFAULT_CLIENT_BLOCK_UNMAPPED_ACTIONS;

    private OnJoinPermissionOperationName onJoinPermissionOperation = OnJoinPermissionOperationName.RECEIVE;

    public SecurityConfig addSecurityInterceptorConfig(SecurityInterceptorConfig interceptorConfig) {
        securityInterceptorConfigs.add(interceptorConfig);
        return this;
    }

    public List<SecurityInterceptorConfig> getSecurityInterceptorConfigs() {
        return securityInterceptorConfigs;
    }

    public void setSecurityInterceptorConfigs(final List<SecurityInterceptorConfig> securityInterceptorConfigs) {
        this.securityInterceptorConfigs = securityInterceptorConfigs;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public SecurityConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public SecurityConfig addMemberLoginModuleConfig(LoginModuleConfig loginModuleConfig) {
        memberLoginModuleConfigs.add(loginModuleConfig);
        return this;
    }

    public SecurityConfig addClientLoginModuleConfig(LoginModuleConfig loginModuleConfig) {
        clientLoginModuleConfigs.add(loginModuleConfig);
        return this;
    }

    public SecurityConfig addClientPermissionConfig(PermissionConfig permissionConfig) {
        clientPermissionConfigs.add(permissionConfig);
        return this;
    }

    public List<LoginModuleConfig> getClientLoginModuleConfigs() {
        return clientLoginModuleConfigs;
    }

    public SecurityConfig setClientLoginModuleConfigs(List<LoginModuleConfig> loginModuleConfigs) {
        this.clientLoginModuleConfigs = loginModuleConfigs;
        return this;
    }

    public List<LoginModuleConfig> getMemberLoginModuleConfigs() {
        return memberLoginModuleConfigs;
    }

    public SecurityConfig setMemberLoginModuleConfigs(List<LoginModuleConfig> memberLoginModuleConfigs) {
        this.memberLoginModuleConfigs = memberLoginModuleConfigs;
        return this;
    }

    public PermissionPolicyConfig getClientPolicyConfig() {
        return clientPolicyConfig;
    }

    public SecurityConfig setClientPolicyConfig(PermissionPolicyConfig policyConfig) {
        this.clientPolicyConfig = policyConfig;
        return this;
    }

    public Set<PermissionConfig> getClientPermissionConfigs() {
        return clientPermissionConfigs;
    }

    public SecurityConfig setClientPermissionConfigs(Set<PermissionConfig> permissions) {
        this.clientPermissionConfigs = permissions;
        return this;
    }

    public CredentialsFactoryConfig getMemberCredentialsConfig() {
        return memberCredentialsConfig;
    }

    public SecurityConfig setMemberCredentialsConfig(CredentialsFactoryConfig credentialsFactoryConfig) {
        this.memberCredentialsConfig = credentialsFactoryConfig;
        return this;
    }

    public OnJoinPermissionOperationName getOnJoinPermissionOperation() {
        return onJoinPermissionOperation;
    }

    public SecurityConfig setOnJoinPermissionOperation(OnJoinPermissionOperationName onJoinPermissionOperation) {
        this.onJoinPermissionOperation = checkNotNull(onJoinPermissionOperation,
                "Existing " + OnJoinPermissionOperationName.class.getSimpleName() + " value has to be provided.");
        return this;
    }

    /**
     * @return a boolean flag indicating whether actions, submitted as tasks in an Executor from clients
     * and have no permission mappings, are blocked or allowed.
     * <p>
     * Executors:
     *
     * <ul>
     * <li>{@link com.hazelcast.core.IExecutorService}
     * <li>{@link com.hazelcast.scheduledexecutor.IScheduledExecutorService}
     * <li>{@link com.hazelcast.durableexecutor.DurableExecutorService}
     * </ul>
     */
    public boolean getClientBlockUnmappedActions() {
        return clientBlockUnmappedActions;
    }

    /**
     * Block or allow actions, submitted as tasks in an Executor from clients and have no permission mappings.
     * <p>
     * Executors:
     *
     * <ul>
     * <li>{@link com.hazelcast.core.IExecutorService}
     * <li>{@link com.hazelcast.scheduledexecutor.IScheduledExecutorService}
     * <li>{@link com.hazelcast.durableexecutor.DurableExecutorService}
     * </ul>
     *
     * @param clientBlockUnmappedActions True: Blocks all actions that have no permission mapping;
     *                                   False: Allows all actions that have no permission mapping
     */
    public SecurityConfig setClientBlockUnmappedActions(boolean clientBlockUnmappedActions) {
        this.clientBlockUnmappedActions = clientBlockUnmappedActions;
        return this;
    }

    @Override
    public String toString() {
        return "SecurityConfig{"
                + "enabled=" + enabled
                + ", memberCredentialsConfig=" + memberCredentialsConfig
                + ", memberLoginModuleConfigs=" + memberLoginModuleConfigs
                + ", clientLoginModuleConfigs=" + clientLoginModuleConfigs
                + ", clientPolicyConfig=" + clientPolicyConfig
                + ", clientPermissionConfigs=" + clientPermissionConfigs
                + ", clientBlockUnmappedActions=" + clientBlockUnmappedActions
                + ", onJoinPermissionOperation=" + onJoinPermissionOperation
                + '}';
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SecurityConfig that = (SecurityConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (clientBlockUnmappedActions != that.clientBlockUnmappedActions) {
            return false;
        }
        if (memberCredentialsConfig != null
                ? !memberCredentialsConfig.equals(that.memberCredentialsConfig)
                : that.memberCredentialsConfig != null) {
            return false;
        }
        if (memberLoginModuleConfigs != null
                ? !memberLoginModuleConfigs.equals(that.memberLoginModuleConfigs)
                : that.memberLoginModuleConfigs != null) {
            return false;
        }
        if (securityInterceptorConfigs != null
                ? !securityInterceptorConfigs.equals(that.securityInterceptorConfigs)
                : that.securityInterceptorConfigs != null) {
            return false;
        }
        if (clientLoginModuleConfigs != null
                ? !clientLoginModuleConfigs.equals(that.clientLoginModuleConfigs)
                : that.clientLoginModuleConfigs != null) {
            return false;
        }
        if (clientPolicyConfig != null
                ? !clientPolicyConfig.equals(that.clientPolicyConfig)
                : that.clientPolicyConfig != null) {
            return false;
        }
        if (onJoinPermissionOperation != that.onJoinPermissionOperation) {
            return false;
        }
        return clientPermissionConfigs != null
                ? clientPermissionConfigs.equals(that.clientPermissionConfigs)
                : that.clientPermissionConfigs == null;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (memberCredentialsConfig != null ? memberCredentialsConfig.hashCode() : 0);
        result = 31 * result + (memberLoginModuleConfigs != null ? memberLoginModuleConfigs.hashCode() : 0);
        result = 31 * result + (securityInterceptorConfigs != null ? securityInterceptorConfigs.hashCode() : 0);
        result = 31 * result + (clientLoginModuleConfigs != null ? clientLoginModuleConfigs.hashCode() : 0);
        result = 31 * result + (clientPolicyConfig != null ? clientPolicyConfig.hashCode() : 0);
        result = 31 * result + (clientPermissionConfigs != null ? clientPermissionConfigs.hashCode() : 0);
        result = 31 * result + (clientBlockUnmappedActions ? 1 : 0);
        result = 31 * result + onJoinPermissionOperation.ordinal();
        return result;
    }
}
