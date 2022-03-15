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
package com.hazelcast.jet.config;

/**
 * Some constants for the {@link JobConfig#getArgument(String)} method.
 */
public final class JobConfigArguments {

    /**
     * The key under which the SQL engine stores the SQL query text in {@link
     * JobConfig#getArgument(String)}. It's set for all jobs backing an SQL
     * query.
     */
    public static final String KEY_SQL_QUERY_TEXT = "__sql.queryText";

    /**
     * The key under which the SQL engine stores whether the job backing an SQL
     * query is bounded or unbounded (returning infinite rows). Use for {@link
     * JobConfig#getArgument(String)}. Contains a {@code Boolean} value.
     */
    public static final String KEY_SQL_UNBOUNDED = "__sql.queryUnbounded";

    private JobConfigArguments() { }
}
