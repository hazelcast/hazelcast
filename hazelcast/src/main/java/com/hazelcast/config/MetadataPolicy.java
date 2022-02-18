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

/**
 * Hazelcast may process objects of supported types ahead of time to
 * create additional metadata about them. This metadata then is used
 * to make querying and indexing faster.
 */
public enum MetadataPolicy {

    /**
     * Hazelcast processes supported objects at the time of creation
     * and updates. This may increase put latency for the specific
     * data structure that this option is configured with.
     */
    CREATE_ON_UPDATE(0),

    /**
     * Turns off metadata creation.
     */
    OFF(1);

    private final int id;

    MetadataPolicy(int id) {
        this.id = id;
    }

    /**
     * Returns enumeration id of this policy. We use id field instead of
     * {@link #ordinal()} because this value is used in client protocol.
     * The ids for the known policies must not be changed.
     * @return id
     */
    public int getId() {
        return this.id;
    }

    /**
     * Returns the MetadataPolicy for the given ID.
     *
     * @return the MetadataPolicy found or null if not found
     */
    public static MetadataPolicy getById(final int id) {
        for (MetadataPolicy policy : values()) {
            if (policy.id == id) {
                return policy;
            }
        }
        return null;
    }
}
