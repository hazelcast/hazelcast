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

enum XmlElements {
    HAZELCAST("hazelcast", false),
    INSTANCE_NAME("instance-name", false),
    IMPORT("import", true),
    CONFIG_REPLACERS("config-replacers", false),
    GROUP("group", false),
    LICENSE_KEY("license-key", false),
    MANAGEMENT_CENTER("management-center", false),
    PROPERTIES("properties", false),
    WAN_REPLICATION("wan-replication", true),
    NETWORK("network", false),
    PARTITION_GROUP("partition-group", false),
    EXECUTOR_SERVICE("executor-service", true),
    DURABLE_EXECUTOR_SERVICE("durable-executor-service", true),
    SCHEDULED_EXECUTOR_SERVICE("scheduled-executor-service", true),
    EVENT_JOURNAL("event-journal", true),
    QUEUE("queue", true),
    MAP("map", true),
    CACHE("cache", true),
    MULTIMAP("multimap", true),
    REPLICATED_MAP("replicatedmap", true),
    LIST("list", true),
    SET("set", true),
    TOPIC("topic", true),
    RELIABLE_TOPIC("reliable-topic", true),
    JOB_TRACKER("jobtracker", true),
    SEMAPHORE("semaphore", true),
    LOCK("lock", true),
    RINGBUFFER("ringbuffer", true),
    ATOMIC_LONG("atomic-long", true),
    ATOMIC_REFERENCE("atomic-reference", true),
    COUNT_DOWN_LATCH("count-down-latch", true),
    LISTENERS("listeners", false),
    SERIALIZATION("serialization", false),
    SERVICES("services", false),
    SECURITY("security", false),
    MEMBER_ATTRIBUTES("member-attributes", false),
    NATIVE_MEMORY("native-memory", false),
    QUORUM("quorum", true),
    LITE_MEMBER("lite-member", false),
    HOT_RESTART_PERSISTENCE("hot-restart-persistence", false),
    USER_CODE_DEPLOYMENT("user-code-deployment", false),
    CARDINALITY_ESTIMATOR("cardinality-estimator", true),
    RELIABLE_ID_GENERATOR("reliable-id-generator", true),
    FLAKE_ID_GENERATOR("flake-id-generator", true),
    CRDT_REPLICATION("crdt-replication", false),
    PN_COUNTER("pn-counter", true),
    ;

    final String name;
    final boolean multipleOccurrence;

    XmlElements(String name, boolean multipleOccurrence) {
        this.name = name;
        this.multipleOccurrence = multipleOccurrence;
    }

    public static boolean canOccurMultipleTimes(String name) {
        for (XmlElements element : values()) {
            if (name.equals(element.name)) {
                return element.multipleOccurrence;
            }
        }
        return false;
    }

    public boolean isEqual(String name) {
        return this.name.equals(name);
    }
}
