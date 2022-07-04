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

package com.hazelcast.sql;

/**
 * The expected statement result type.
 *
 * @since 4.2
 */
public enum SqlExpectedResultType {
    /** The statement may produce either rows or an update count. */
    ANY((byte) 0),

    /** The statement must produce rows. An exception is thrown is the statement produces an update count. */
    ROWS((byte) 1),

    /** The statement must produce an update count. An exception is thrown is the statement produces rows. */
    UPDATE_COUNT((byte) 2);

    private final byte id;

    SqlExpectedResultType(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static SqlExpectedResultType fromId(byte id) {
        switch (id) {
            case 0:
                return ANY;
            case 1:
                return ROWS;
            default:
                assert id == UPDATE_COUNT.id;
                return UPDATE_COUNT;
        }
    }
}
