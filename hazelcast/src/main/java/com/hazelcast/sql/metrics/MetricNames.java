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

package com.hazelcast.sql.metrics;

/**
 * This class contains names of metrics related to the SQL module.
 *
 * @since 5.3
 */
public final class MetricNames {

    /**
     * Name of SQL module used in the MODULE metric tag.
     */
    public static final String MODULE_TAG = "sql";

    /**
     * Total number of queries executed by the SQL engine.
     */
    public static final String FAST_QUERIES_EXECUTED = "sql.fast.queries.executed";

    private MetricNames() { }
}
