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

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.security.ICredentialsFactory;

/**
 * Contains configuration for Security
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class SecurityConfig {

    private static final boolean DEFAULT_CLIENT_BLOCK_UNMAPPED_ACTIONS = true;

    private boolean enabled;

    private List<SecurityInterceptorConfig> securityInterceptorConfigs = new ArrayList<SecurityInterceptorConfig>();

    private PermissionPolicyConfig clientPolicyConfig = new PermissionPolicyConfig();

    private Set<PermissionConfig> clientPermissionConfigs = new HashSet<PermissionConfig>();

    private Map<String, RealmConfig> realmConfigs = new HashMap<>();

    private String memberRealm;
    private String clientRealm;

    private boolean clientBlockUnmappedActions = DEFAULT_CLIENT_BLOCK_UNMAPPED_ACTIONS;

    private OnJoinPermissionOperationName onJoinPermissionOperation = OnJoinPermissionOperationName.RECEIVE;

    public SecurityConfig addSecurityInterceptorConfig(SecurityInterceptorConfig interceptorConfig) {
        securityInterceptorConfigs.add(interceptorConfig);
        return this;
    }

    public List<SecurityInterceptorConfig> getSecurityInterceptorConfigs() {
        return securityInterceptorConfigs;
    }

    public SecurityConfig setSecurityInterceptorConfigs(List<SecurityInterceptorConfig> securityInterceptorConfigs) {
        this.securityInterceptorConfigs = securityInterceptorConfigs;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public SecurityConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public SecurityConfig addClientPermissionConfig(PermissionConfig permissionConfig) {
        clientPermissionConfigs.add(permissionConfig);
        return this;
    }

    public SecurityConfig addRealmConfig(String realmName, RealmConfig realmConfig) {
        realmConfigs.put(realmName, realmConfig);
        return this;
    }

    public RealmConfig getRealmConfig(String realmName) {
        return realmName == null ? null : realmConfigs.get(realmName);
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

    public ICredentialsFactory getRealmCredentialsFactory(String realmName) {
        return getRealmConfigOrDefault(realmName).asCredentialsFactory();
    }

    public LoginModuleConfig[] getRealmLoginModuleConfigs(String realmName) {
        return getRealmConfigOrDefault(realmName).asLoginModuleConfigs();
    }

    public OnJoinPermissionOperationName getOnJoinPermissionOperation() {
        return onJoinPermissionOperation;
    }

    public SecurityConfig setOnJoinPermissionOperation(OnJoinPermissionOperationName onJoinPermissionOperation) {
        this.onJoinPermissionOperation = checkNotNull(onJoinPermissionOperation,
                "Existing " + OnJoinPermissionOperationName.class.getSimpleName() + " value has to be provided.");
        return this;
    }

    public Map<String, RealmConfig> getRealmConfigs() {
        return realmConfigs;
    }

    public void setRealmConfigs(Map<String, RealmConfig> realmConfigs) {
        this.realmConfigs = realmConfigs;
    }

    public String getMemberRealm() {
        return memberRealm;
    }

    public SecurityConfig setMemberRealm(String memberRealm) {
        this.memberRealm = memberRealm;
        return this;
    }

    public String getClientRealm() {
        return clientRealm;
    }

    public SecurityConfig setClientRealm(String clientRealm) {
        this.clientRealm = clientRealm;
        return this;
    }

    public SecurityConfig setMemberRealmConfig(String realmName, RealmConfig realmConfig) {
        addRealmConfig(realmName, realmConfig);
        setMemberRealm(realmName);
        return this;
    }

    public SecurityConfig setClientRealmConfig(String realmName, RealmConfig realmConfig) {
        addRealmConfig(realmName, realmConfig);
        setClientRealm(realmName);
        return this;
    }

    /**
     * Returns if the given name is a valid realm name.
     *
     * @param name realm name to be checked
     * @return {@code true} if realm with given name exists, {@code false} otherwise.
     */
    public boolean isRealm(String name) {
        return name != null && realmConfigs.containsKey(name);
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
        return "SecurityConfig [enabled=" + enabled + ", securityInterceptorConfigs=" + securityInterceptorConfigs
                + ", clientPolicyConfig=" + clientPolicyConfig + ", clientPermissionConfigs=" + clientPermissionConfigs
                + ", realmConfigs=" + realmConfigs + ", memberRealm=" + memberRealm + ", clientRealm=" + clientRealm
                + ", clientBlockUnmappedActions=" + clientBlockUnmappedActions + ", onJoinPermissionOperation="
                + onJoinPermissionOperation + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientBlockUnmappedActions, clientPermissionConfigs, clientPolicyConfig, clientRealm, enabled,
                memberRealm, onJoinPermissionOperation, realmConfigs, securityInterceptorConfigs);
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
        SecurityConfig other = (SecurityConfig) obj;
        return clientBlockUnmappedActions == other.clientBlockUnmappedActions
                && Objects.equals(clientPermissionConfigs, other.clientPermissionConfigs)
                && Objects.equals(clientPolicyConfig, other.clientPolicyConfig)
                && Objects.equals(clientRealm, other.clientRealm) && enabled == other.enabled
                && Objects.equals(memberRealm, other.memberRealm)
                && onJoinPermissionOperation == other.onJoinPermissionOperation
                && Objects.equals(realmConfigs, other.realmConfigs)
                && Objects.equals(securityInterceptorConfigs, other.securityInterceptorConfigs);
    }

    private RealmConfig getRealmConfigOrDefault(String realmName) {
        RealmConfig realmConfig = realmName == null ? null : realmConfigs.get(realmName);
        return realmConfig == null ? RealmConfig.DEFAULT_REALM : realmConfig;
    }
}
