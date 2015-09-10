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

package com.hazelcast.cluster;

/**
 * Used to identify the type of member attribute change, either PUT or REMOVED
 *
 * @since 3.2.RC-2
 */
public enum MemberAttributeOperationType {

    /**
     * The type when an attributed is put.
     */
    PUT(1),

    /**
     * The type when an attribute is removed.
     */
    REMOVE(2);

    private final int id;

    MemberAttributeOperationType(int i) {
        this.id = i;
    }

    /**
     * Gets the id that uniquely identifies this MemberAttributeOperationType.
     *
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Gets MemberAttributeOperationType for the given id.
     *
     * @param id the id.
     * @return the found MemberAttributeOperationType
     * @throws IllegalArgumentException if the MemberAttributeOperationType for the given id can't be found.
     */
    public static MemberAttributeOperationType getValue(int id) {
        for (MemberAttributeOperationType operationType : values()) {
            if (operationType.id == id) {
                return operationType;
            }
        }
        throw new IllegalArgumentException("No MemberAttributeOperationType for id: " + id);
    }
}
