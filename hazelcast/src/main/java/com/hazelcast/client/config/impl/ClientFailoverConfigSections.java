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
public enum ClientFailoverConfigSections {
    CLIENT_FAILOVER("hazelcast-client-failover", false),
    CLIENTS("clients", false),
    TRY_COUNT("try-count", false);

    final boolean multipleOccurrence;
    private final String name;

    ClientFailoverConfigSections(String name, boolean multipleOccurrence) {
        this.name = name;
        this.multipleOccurrence = multipleOccurrence;
    }

    public static boolean canOccurMultipleTimes(String name) {
        for (ClientFailoverConfigSections element : values()) {
            if (name.equals(element.name)) {
                return element.multipleOccurrence;
            }
        }
        return true;
    }

    public boolean isEqual(String name) {
        return this.name.equals(name);
    }

    public String getName() {
        return name;
    }
}
