/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

enum ClientXmlElements {
    HAZELCAST_CLIENT("hazelcast-client", false),
    IMPORT("import", true),
    SECURITY("security", false),
    PROXY_FACTORIES("proxy-factories", false),
    PROPERTIES("properties", false),
    SERIALIZATION("serialization", false),
    NATIVE_MEMORY("native-memory", false),
    GROUP("group", false),
    LISTENERS("listeners", false),
    NETWORK("network", false),
    LOAD_BALANCER("load-balancer", false),
    NEAR_CACHE("near-cache", true),
    QUERY_CACHES("query-caches", false),
    EXECUTOR_POOL_SIZE("executor-pool-size", false),
    LICENSE_KEY("license-key", false),
    INSTANCE_NAME("instance-name", false),
    CONNECTION_STRATEGY("connection-strategy", false),
    USER_CODE_DEPLOYMENT("user-code-deployment", false),
    RELIABLE_ID_GENERATOR("reliable-id-generator", true);

    final String name;
    final boolean multipleOccurrence;

    ClientXmlElements(String name, boolean multipleOccurrence) {
        this.name = name;
        this.multipleOccurrence = multipleOccurrence;
    }

    public static boolean canOccurMultipleTimes(String name) {
        for (ClientXmlElements element : values()) {
            if (name.equals(element.name)) {
                return element.multipleOccurrence;
            }
        }
        return true;
    }

    public boolean isEqual(String name) {
        return this.name.equals(name);
    }

}
