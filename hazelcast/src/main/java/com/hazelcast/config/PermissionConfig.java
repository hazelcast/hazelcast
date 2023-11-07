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

package com.hazelcast.config;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.security.permission.AllPermissions;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.security.permission.CPMapPermission;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.CardinalityEstimatorPermission;
import com.hazelcast.security.permission.ConfigPermission;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.security.permission.DurableExecutorServicePermission;
import com.hazelcast.security.permission.ExecutorServicePermission;
import com.hazelcast.security.permission.FlakeIdGeneratorPermission;
import com.hazelcast.security.permission.JobPermission;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.security.permission.ManagementPermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.security.permission.PNCounterPermission;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.security.permission.ReliableTopicPermission;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.security.permission.SqlPermission;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.security.permission.UserCodeDeploymentPermission;

import java.io.IOException;
import java.security.Permission;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static java.util.Collections.newSetFromMap;

/**
 * Contains the configuration for a permission.
 */
public class PermissionConfig implements IdentifiedDataSerializable, Versioned {

    private PermissionType type;
    private String name;
    private String principal;
    private Set<String> endpoints = newSetFromMap(new ConcurrentHashMap<>());
    private Set<String> actions = newSetFromMap(new ConcurrentHashMap<>());
    private boolean deny;

    public PermissionConfig() {
    }

    public PermissionConfig(PermissionType type, String name, String principal) {
        this.type = type;
        this.name = name;
        this.principal = principal;
    }

    public PermissionConfig(PermissionConfig permissionConfig) {
        this.type = permissionConfig.type;
        this.name = permissionConfig.getName();
        this.principal = permissionConfig.getPrincipal();
        for (String endpoint : permissionConfig.getEndpoints()) {
            this.endpoints.add(endpoint);
        }
        for (String action : permissionConfig.getActions()) {
            this.actions.add(action);
        }
    }

    /**
     * Type of permission
     */
    public enum PermissionType {
        /**
         * Type backed by {@link AllPermissions}, which implies all checked permissions.
         */
        ALL("all-permissions", AllPermissions.class),
        /**
         * Map
         */
        MAP("map-permission", MapPermission.class),
        /**
         * Queue
         */
        QUEUE("queue-permission", QueuePermission.class),
        /**
         * Topic
         */
        TOPIC("topic-permission", TopicPermission.class),
        /**
         * MultiMap
         */
        MULTIMAP("multimap-permission", MultiMapPermission.class),
        /**
         * List
         */
        LIST("list-permission", ListPermission.class),
        /**
         * Set
         */
        SET("set-permission", SetPermission.class),
        /**
         * Flake ID generator
         */
        FLAKE_ID_GENERATOR("flake-id-generator-permission", FlakeIdGeneratorPermission.class),
        /**
         * Lock
         */
        LOCK("lock-permission", LockPermission.class),
        /**
         * Atomic long
         */
        ATOMIC_LONG("atomic-long-permission", AtomicLongPermission.class),
        /**
         * Atomic long
         */
        ATOMIC_REFERENCE("atomic-reference-permission", AtomicReferencePermission.class),
        /**
         * Countdown Latch
         */
        COUNTDOWN_LATCH("countdown-latch-permission", CountDownLatchPermission.class),
        /**
         * Semaphore
         */
        SEMAPHORE("semaphore-permission", SemaphorePermission.class),
        /**
         * Executor Service
         */
        EXECUTOR_SERVICE("executor-service-permission", ExecutorServicePermission.class),
        /**
         * Transaction
         */
        TRANSACTION("transaction-permission", TransactionPermission.class),
        /**
         * Durable Executor Service
         */
        DURABLE_EXECUTOR_SERVICE("durable-executor-service-permission", DurableExecutorServicePermission.class),
        /**
         * Cardinality Estimator
         */
        CARDINALITY_ESTIMATOR("cardinality-estimator-permission", CardinalityEstimatorPermission.class),
        /**
         * Scheduled executor service
         */
        SCHEDULED_EXECUTOR("scheduled-executor-permission", ScheduledExecutorPermission.class),
        /**
         * JCache/ICache
         */
        CACHE("cache-permission", CachePermission.class),
        /**
         * User code deployment
         */
        USER_CODE_DEPLOYMENT("user-code-deployment-permission", UserCodeDeploymentPermission.class),
        /**
         * Configuration permission
         */
        CONFIG("config-permission", ConfigPermission.class),
        /**
         * CRDT PN Counter
         */
        PN_COUNTER("pn-counter-permission", PNCounterPermission.class),
        /**
         * RingBuffer
         */
        RING_BUFFER("ring-buffer-permission", RingBufferPermission.class),
        /**
         * ReliableTopic
         */
        RELIABLE_TOPIC("reliable-topic-permission", ReliableTopicPermission.class),
        /**
         * ReplicatedMap
         */
        REPLICATEDMAP("replicatedmap-permission", ReplicatedMapPermission.class),
        /**
         * Cluster Management
         */
        MANAGEMENT("management-permission", ManagementPermission.class),
        /**
         * Jet Job permission
         */
        JOB("job-permission", JobPermission.class),
        /**
         * Jet Connector permission
         */
        CONNECTOR("connector-permission", ConnectorPermission.class),
        /**
         * Specific SQL permissions
         */
        SQL("sql-permission", SqlPermission.class),
        /**
         * CP Map permissions
         */
        CPMAP("cpmap-permission", CPMapPermission.class);

        private final String nodeName;
        private final String className;

        PermissionType(String nodeName, Class<? extends Permission> permClass) {
            this.nodeName = nodeName;
            this.className = permClass.getName();
        }

        public static PermissionType getType(String nodeName) {
            return findPermissionTypeByFilter(v -> v.nodeName.equals(nodeName));
        }

        public static PermissionType getTypeByPermissionClassName(String permissionClassname) {
            return findPermissionTypeByFilter(v -> v.className.equals(permissionClassname));
        }

        public String getNodeName() {
            return nodeName;
        }

        private static PermissionType findPermissionTypeByFilter(Predicate<PermissionType> filterPredicate) {
            return Arrays.stream(values()).filter(filterPredicate).findFirst().orElse(null);
        }
    }

    public PermissionConfig addEndpoint(String endpoint) {
        endpoints.add(endpoint);
        return this;
    }

    public PermissionConfig addAction(String action) {
        actions.add(action);
        return this;
    }

    public PermissionType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getPrincipal() {
        return principal;
    }

    public Set<String> getEndpoints() {
        return endpoints;
    }

    public Set<String> getActions() {
        return actions;
    }

    public PermissionConfig setType(PermissionType type) {
        this.type = type;
        return this;
    }

    public PermissionConfig setName(String name) {
        this.name = name;
        return this;
    }

    public PermissionConfig setPrincipal(String principal) {
        this.principal = principal;
        return this;
    }

    public PermissionConfig setActions(Set<String> actions) {
        this.actions = actions;
        return this;
    }

    public PermissionConfig setEndpoints(Set<String> endpoints) {
        this.endpoints = endpoints;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.PERMISSION_CONFIG;
    }

    /**
     * Returns {@code true} when the permission should be subtracted (denied) instead of added (granted).
     * @return {@code true} for deny permissions
     * @since 5.4
     */
    public boolean isDeny() {
        return deny;
    }

    /**
     * Configures if this permission config is for a grant ({@code false}, default) permission or deny ({@code true})
     * @param deny value to set
     * @return this instance of the {@link PermissionConfig}
     * @since 5.4
     */
    public PermissionConfig setDeny(boolean deny) {
        this.deny = deny;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(type.getNodeName());
        out.writeString(name);
        if (StringUtil.isNullOrEmptyAfterTrim(principal)) {
            out.writeString("*");
        } else {
            out.writeString(principal);
        }

        out.writeInt(endpoints.size());
        for (String endpoint : endpoints) {
            out.writeString(endpoint);
        }

        out.writeInt(actions.size());
        for (String action : actions) {
            out.writeString(action);
        }

        if (out.getVersion().isGreaterOrEqual(Versions.V5_4)) {
            out.writeBoolean(deny);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = PermissionType.getType(in.readString());
        name = in.readString();
        principal = in.readString();

        int endpointsSize = in.readInt();
        if (endpointsSize != 0) {
            Set<String> endpoints = createHashSet(endpointsSize);
            for (int i = 0; i < endpointsSize; i++) {
                endpoints.add(in.readString());
            }
            this.endpoints = endpoints;
        }

        int actionsSize = in.readInt();
        if (actionsSize != 0) {
            Set<String> actions = createHashSet(actionsSize);
            for (int i = 0; i < actionsSize; i++) {
                actions.add(in.readString());
            }
            this.actions = actions;
        }

        if (in.getVersion().isGreaterOrEqual(Versions.V5_4)) {
            deny = in.readBoolean();
        }
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PermissionConfig)) {
            return false;
        }

        PermissionConfig that = (PermissionConfig) o;

        if (type != that.type) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (principal != null ? !principal.equals(that.principal) : that.principal != null) {
            return false;
        }
        if (endpoints != null ? !endpoints.equals(that.endpoints) : that.endpoints != null) {
            return false;
        }
        if (deny != that.deny) {
            return false;
        }
        return actions != null ? actions.equals(that.actions) : that.actions == null;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (principal != null ? principal.hashCode() : 0);
        result = 31 * result + (endpoints != null ? endpoints.hashCode() : 0);
        result = 31 * result + (actions != null ? actions.hashCode() : 0);
        result = 31 * result + Boolean.hashCode(deny);
        return result;
    }

    @Override
    public String toString() {
        return "PermissionConfig{"
                + "type=" + type
                + ", name='" + name + '\''
                + ", clientUuid='" + principal + '\''
                + ", endpoints=" + endpoints
                + ", actions=" + actions
                + ", deny=" + deny
                + '}';
    }
}
