/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.config;

enum XmlElements {
    HAZELCAST("hazelcast", false),
    IMPORT("import", true),
    GROUP("group", false),
    LICENSE_KEY("license-key", false),
    MANAGEMENT_CENTER("management-center", false),
    PROPERTIES("properties", false),
    WAN_REPLICATION("wan-replication", true),
    NETWORK("network", false),
    PARTITION_GROUP("partition-group", false),
    EXECUTOR_SERVICE("executor-service", true),
    QUEUE("queue", true),
    MAP("map", true),
    MULTIMAP("multimap", true),
    REPLICATED_MAP("replicatedmap", true),
    LIST("list", true),
    SET("set", true),
    TOPIC("topic", true),
    JOB_TRACKER("jobtracker", true),
    SEMAPHORE("semaphore", true),
    LISTENERS("listeners", false),
    SERIALIZATION("serialization", false),
    SERVICES("services", false),
    SECURITY("security", false),
    MEMBER_ATTRIBUTES("member-attributes", false),
    OFF_HEAP_MEMORY("off-heap-memory", false);

    final String name;
    final boolean multipleOccurance;

    XmlElements(String name, boolean multipleOccurance) {
        this.name = name;
        this.multipleOccurance = multipleOccurance;
    }

    public static boolean canOccurMultipleTimes(String name) {
        for (XmlElements element : values()) {
            if (name.equals(element.name)) {
                return element.multipleOccurance;
            }
        }
        return false;
    }

    public boolean isEqual(String name) {
        return this.name.equals(name);
    }

}
