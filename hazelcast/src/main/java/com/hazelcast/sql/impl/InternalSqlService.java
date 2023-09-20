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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.sql.impl.state.QueryClientStateRegistry;

import javax.annotation.Nonnull;

/**
 * Non-public methods that the `hazelcast` module needs to call in the
 * implementation in the `hazelcast-sql` module, extending the public {@link
 * SqlService}.
 */
public interface InternalSqlService extends SqlService {

    String SERVICE_NAME = "hz:impl:sqlService";

    void start();
    void reset();
    void shutdown();

    void closeOnError(QueryId queryId);

    SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext);
    SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext, QueryId queryId);
    SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext, QueryId queryId, boolean skipStats);

    QueryClientStateRegistry getClientStateRegistry();

    long getSqlQueriesSubmittedCount();
    long getSqlStreamingQueriesExecutedCount();

    String mappingDdl(String name);
}
