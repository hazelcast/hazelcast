/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.annotation.PrivateApi;

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

    /**
     * The key under which the SQL engine stores detected partitions to apply
     * member pruning technique.
     */
    public static final String KEY_REQUIRED_PARTITIONS = "__sql.requiredPartitions";

    /**
     * The key under which caller marks analyzed job.
     * The reason for not having a separate flag in {@link JobConfig}
     * is that we want to preserve Jet's independence from SQL.
     * <p>
     * The value for that key supposed to have {@link Boolean} 'false' value
     * to prevent job suspension.
     * <p>
     * By default, any normal Jet job is suspendable.
     */
    public static final String KEY_JOB_IS_SUSPENDABLE = "__jet.jobIsSuspendable";

    /**
     * The key under which the associated User Code Namespace for this job is stored.
     * <p>
     * <b>NOTE:</b> The User Code Namespace defined by this key should only be used if there
     * is no {@link ClassLoader} factory defined at {@link JobConfig#getClassLoaderFactory()}.
     * If {@link JobConfig#getClassLoaderFactory()} is defined in addition to a User Code
     * Namespace being provided, an {@link com.hazelcast.config.InvalidConfigurationException}
     * will be thrown at Job creation.
     * <p>
     * This argument should be set by calling {@link JobConfig#setUserCodeNamespace(String)}.
     * <p>
     * By default, this key will not exist and a User Code Namespace will not be used.
     */
    @PrivateApi
    public static final String KEY_USER_CODE_NAMESPACE = "__jet.userCodeNamespace";

    private JobConfigArguments() {
    }
}
