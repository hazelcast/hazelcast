/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * Determines acknowledge waiting type of wan replication operation invocation
 */
public enum WanAcknowledgeType {

    /**
     * ACK when operation is invoked successfully on target cluster
     */
    ACK_ON_TRANSMIT(0),

    /**
     * Wait till the operation is complete on target cluster
     */
    ACK_ON_OPERATION_COMPLETE(1);

    private final int id;

    WanAcknowledgeType(int id) {
        this.id = id;
    }

    /**
     * Gets the id for the given {@link WanAcknowledgeType}.
     *
     * This reason this id is used instead of an the ordinal value is that the ordinal value is more prone to changes due to
     * reordering.
     *
     * @return the id.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the {@link WanAcknowledgeType} for the given id.
     *
     * @return the {@link WanAcknowledgeType} found or null if not found
     */
    public static WanAcknowledgeType getById(final int id) {
        for (WanAcknowledgeType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}
