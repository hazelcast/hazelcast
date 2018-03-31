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

package com.hazelcast.config;

import java.util.HashSet;
import java.util.Set;

/**
 * Contains the configuration for a permission.
 */
public class PermissionConfig {

    private PermissionType type;
    private String name;
    private String principal;
    private Set<String> endpoints = new HashSet<String>();
    private Set<String> actions = new HashSet<String>();

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
         * Map
         */
        MAP("map-permission"),
        /**
         * Queue
         */
        QUEUE("queue-permission"),
        /**
         * Topic
         */
        TOPIC("topic-permission"),
        /**
         * MultiMap
         */
        MULTIMAP("multimap-permission"),
        /**
         * List
         */
        LIST("list-permission"),
        /**
         * Set
         */
        SET("set-permission"),
        /**
         * ID generator
         */
        ID_GENERATOR("id-generator-permission"),
        /**
         * Flake ID generator
         */
        FLAKE_ID_GENERATOR("flake-id-generator-permission"),
        /**
         * Lock
         */
        LOCK("lock-permission"),
        /**
         * Atomic long
         */
        ATOMIC_LONG("atomic-long-permission"),
        /**
         * Countdown Latch
         */
        COUNTDOWN_LATCH("countdown-latch-permission"),
        /**
         * Semaphore
         */
        SEMAPHORE("semaphore-permission"),
        /**
         * Executor Service
         */
        EXECUTOR_SERVICE("executor-service-permission"),
        /**
         * Transaction
         */
        TRANSACTION("transaction-permission"),
        /**
         * Durable Executor Service
         */
        DURABLE_EXECUTOR_SERVICE("durable-executor-service-permission"),
        /**
         * Cardinality Estimator
         */
        CARDINALITY_ESTIMATOR("cardinality-estimator-permission"),
        /**
         * Scheduled executor service
         */
        SCHEDULED_EXECUTOR("scheduled-executor-permission"),
        /**
         * JCache/ICache
         */
        CACHE("cache-permission"),
        /**
         * User code deployment
         */
        USER_CODE_DEPLOYMENT("user-code-deployment-permission"),
        /**
         * All
         */
        ALL("all-permissions"),
        /**
         * Configuration permission
         */
        CONFIG("config-permission"),
        /**
         * CRDT PN Counter
         */
        PN_COUNTER("pn-counter-permission");

        private final String nodeName;

        PermissionType(String nodeName) {
            this.nodeName = nodeName;
        }

        public static PermissionType getType(String nodeName) {
            for (PermissionType type : PermissionType.values()) {
                if (nodeName.equals(type.getNodeName())) {
                    return type;
                }
            }
            return null;
        }

        public String getNodeName() {
            return nodeName;
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
        return actions != null ? actions.equals(that.actions) : that.actions == null;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (principal != null ? principal.hashCode() : 0);
        result = 31 * result + (endpoints != null ? endpoints.hashCode() : 0);
        result = 31 * result + (actions != null ? actions.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PermissionConfig{"
                + "type=" + type
                + ", name='" + name + '\''
                + ", principal='" + principal + '\''
                + ", endpoints=" + endpoints
                + ", actions=" + actions
                + '}';
    }
}
