/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

/**
 * The configuration when to retry query that fails with reasons:
 * <ul>
 *     <li>{@link com.hazelcast.sql.impl.SqlErrorCode#CONNECTION_PROBLEM}</li>
 *     <li>{@link com.hazelcast.sql.impl.SqlErrorCode#PARTITION_DISTRIBUTION}</li>
 *     <li>{@link com.hazelcast.sql.impl.SqlErrorCode#TOPOLOGY_CHANGE}</li>
 *     <li>{@link com.hazelcast.sql.impl.SqlErrorCode#RESTARTABLE_ERROR}</li>
 * </ul>
 *
 * @since 5.2
 */
public enum ClientSqlResubmissionMode {

    /**
     * If a query fails, the failure is immediately forwarded to the caller.
     */
    NEVER,

    /**
     * The query will be retried if:
     * <ul>
     *   <li>no rows were received yet</li>
     *   <li>the SQL text starts with `SELECT` (case-insensitive, ignoring white space)</li>
     * </ul>
     */
    RETRY_SELECTS,

    /**
     * The query will be retried if the SQL text starts with `SELECT` (case-insensitive, ignoring white space). If some rows
     * were received they are going to be duplicated.
     */
    RETRY_SELECTS_ALLOW_DUPLICATES,

    /**
     * All queries will be retried after a failure.
     */
    RETRY_ALL
}
