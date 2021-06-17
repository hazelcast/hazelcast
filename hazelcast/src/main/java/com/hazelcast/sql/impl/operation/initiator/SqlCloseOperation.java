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

package com.hazelcast.sql.impl.operation.initiator;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlInternalService;

import java.util.concurrent.CompletableFuture;

public class SqlCloseOperation extends SqlQueryOperation {
    public SqlCloseOperation() {
    }

    public SqlCloseOperation(QueryId queryId) {
        super(queryId);
    }

    @Override
    protected CompletableFuture<?> doRun() throws Exception {
        SqlInternalService service = getNodeEngine().getSqlService().getInternalService();
        service.getClientStateRegistry().close(endpoint.getUuid(), getQueryId());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.OPERATION_CLOSE;
    }
}
