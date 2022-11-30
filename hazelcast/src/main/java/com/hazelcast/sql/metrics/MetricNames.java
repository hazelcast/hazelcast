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

package com.hazelcast.sql.metrics;

/**
 * This class contains names of metrics related to SQL module.
 * @since 5.3
 */
public final class MetricNames {

    /**
     * Name of SQL module typically used in the MODULE metric tag.
     */
    public static final String MODULE_TAG = "sql";
    // TODO: rename to sql.quick.queries.executed or add increments into all queries?
    /**
     * Total number of queries executed by the SQL engine.
     */
    public static final String QUERIES_EXECUTED = "sql.queries.executed";

    private MetricNames() { }
}
