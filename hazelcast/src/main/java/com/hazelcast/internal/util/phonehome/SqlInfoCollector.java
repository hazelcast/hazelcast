/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.sql.impl.InternalSqlService;

import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.SQL_QUERIES_SUBMITTED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.SQL_STREAMING_QUERIES_EXECUTED;

public class SqlInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        InternalSqlService sqlService = node.getNodeEngine().getSqlService();

        long sqlQueriesSubmittedCount = sqlService.getSqlQueriesSubmittedCount();
        metricsConsumer.accept(SQL_QUERIES_SUBMITTED, String.valueOf(sqlQueriesSubmittedCount));

        long sqlStreamingQueriesExecutedCount = sqlService.getSqlStreamingQueriesExecutedCount();
        metricsConsumer.accept(SQL_STREAMING_QUERIES_EXECUTED, String.valueOf(sqlStreamingQueriesExecutedCount));
    }
}
