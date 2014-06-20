/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

public class PermissionConfig {

    private PermissionType type;
    private String name;
    private String principal;
    private Set<String> endpoints;
    private Set<String> actions;

    public PermissionConfig() {
        super();
    }

    public PermissionConfig(PermissionType type, String name, String principal) {
        super();
        this.type = type;
        this.name = name;
        this.principal = principal;
    }

    public enum PermissionType {
        MAP("map-permission"),
        QUEUE("queue-permission"),
        TOPIC("topic-permission"),
        MULTIMAP("multimap-permission"),
        LIST("list-permission"),
        SET("set-permission"),
        ID_GENERATOR("id-generator-permission"),
        LOCK("lock-permission"),
        ATOMIC_LONG("atomic-long-permission"),
        COUNTDOWN_LATCH("countdown-latch-permission"),
        SEMAPHORE("semaphore-permission"),
        EXECUTOR_SERVICE("executor-service-permission"),
        TRANSACTION("transaction-permission"),
        ALL("all-permissions");

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
        if (endpoints == null) {
            endpoints = new HashSet<String>();
        }
        endpoints.add(endpoint);
        return this;
    }

    public PermissionConfig addAction(String action) {
        if (actions == null) {
            actions = new HashSet<String>();
        }
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
        if (endpoints == null) {
            endpoints = new HashSet<String>();
        }
        return endpoints;
    }

    public Set<String> getActions() {
        if (actions == null) {
            actions = new HashSet<String>();
        }
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
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("PermissionConfig");
        sb.append("{type=").append(type);
        sb.append(", name='").append(name).append('\'');
        sb.append(", principal='").append(principal).append('\'');
        sb.append(", endpoints=").append(endpoints);
        sb.append(", actions=").append(actions);
        sb.append('}');
        return sb.toString();
    }
}
