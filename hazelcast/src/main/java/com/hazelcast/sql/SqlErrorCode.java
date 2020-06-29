/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * Error codes used in Hazelcast SQL.
 */
public final class SqlErrorCode {
    /** Generic error. */
    public static final int GENERIC = -1;

    /** Member cannot be reached. */
    public static final int MEMBER_CONNECTION = 1001;

    /** Member has left the topology. */
    public static final int MEMBER_LEAVE = 1002;

    /** Query was cancelled due to user request. */
    public static final int CANCELLED_BY_USER = 1003;

    /** Query was cancelled due to timeout. */
    public static final int TIMEOUT = 1004;

    /** An error caused by concurrent migration of partition to other member. */
    public static final int PARTITION_MIGRATED = 1005;

    /** An error caused concurrent destroy of a map. */
    public static final int MAP_DESTROYED = 1006;

    /** Map loading is not finished yet. */
    public static final int MAP_LOADING_IN_PROGRESS = 1007;

    /** Generic parsing error. */
    public static final int PARSING = 1008;

    /** An error with data conversion or transformation. */
    public static final int DATA_EXCEPTION = 2000;

    private SqlErrorCode() {
        // No-op.
    }
}
