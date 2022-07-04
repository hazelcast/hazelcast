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

package com.hazelcast.client.config.impl;

/**
 * Configuration sections for the clients shared by XML and YAML based
 * configurations
 */
public enum ClientConfigSections {
    HAZELCAST_CLIENT("hazelcast-client", false),
    IMPORT("import", true),
    SECURITY("security", false),
    PROXY_FACTORIES("proxy-factories", false),
    PROPERTIES("properties", false),
    SERIALIZATION("serialization", false),
    NATIVE_MEMORY("native-memory", false),
    LISTENERS("listeners", false),
    NETWORK("network", false),
    LOAD_BALANCER("load-balancer", false),
    NEAR_CACHE("near-cache", true),
    QUERY_CACHES("query-caches", false),
    BACKUP_ACK_TO_CLIENT("backup-ack-to-client-enabled", false),
    INSTANCE_NAME("instance-name", false),
    CONNECTION_STRATEGY("connection-strategy", false),
    USER_CODE_DEPLOYMENT("user-code-deployment", false),
    FLAKE_ID_GENERATOR("flake-id-generator", true),
    RELIABLE_TOPIC("reliable-topic", true),
    LABELS("client-labels", false),
    CLUSTER_NAME("cluster-name", false),
    METRICS("metrics", false),
    INSTANCE_TRACKING("instance-tracking", false);

    final boolean multipleOccurrence;
    private final String name;

    ClientConfigSections(String name, boolean multipleOccurrence) {
        this.name = name;
        this.multipleOccurrence = multipleOccurrence;
    }

    public static boolean canOccurMultipleTimes(String name) {
        for (ClientConfigSections element : values()) {
            if (name.equals(element.name)) {
                return element.multipleOccurrence;
            }
        }
        return true;
    }

    public String getName() {
        return name;
    }
}
