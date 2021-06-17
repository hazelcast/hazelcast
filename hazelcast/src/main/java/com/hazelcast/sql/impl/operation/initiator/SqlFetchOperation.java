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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlInternalService;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class SqlFetchOperation extends SqlQueryOperation {

    private int cursorBufferSize;

    public SqlFetchOperation() {
    }

    public SqlFetchOperation(QueryId queryId, int cursorBufferSize) {
        super(queryId);
        this.cursorBufferSize = cursorBufferSize;
    }

    @Override
    protected CompletableFuture<?> doRun() throws Exception {
        SqlInternalService service = getNodeEngine().getSqlService().getInternalService();

        return CompletableFuture.completedFuture(service.getClientStateRegistry().fetch(
                getQueryId(),
                cursorBufferSize,
                ((NodeEngineImpl) getNodeEngine()).getNode().getSerializationService()));
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.OPERATION_FETCH;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(cursorBufferSize);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        cursorBufferSize = in.readInt();
    }
}
