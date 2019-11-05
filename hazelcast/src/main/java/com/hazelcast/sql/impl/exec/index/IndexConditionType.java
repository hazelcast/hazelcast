/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.index;

/**
 * Index condition type.
 */
public enum IndexConditionType {
    EQUALS(0),
    GREATER_THAN(1),
    GREATER_THAN_OR_EQUAL(2),
    LESS_THAN(3),
    LESS_THAN_OR_EQUAL(4);

    private final int id;

    IndexConditionType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static IndexConditionType getById(int id) {
        // TODO: AFAIK values() creates an intermediate array, isn't it?
        for (IndexConditionType value : values()) {
            if (value.id == id) {
                return value;
            }
        }

        // TODO: Unsupported type! Handle properly for serialization.
        return null;
    }
}
