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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlServiceImpl;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

public abstract class SqlQueryOperation extends Operation implements IdentifiedDataSerializable {

    private QueryId queryId;

    public SqlQueryOperation() {
    }

    public SqlQueryOperation(QueryId queryId) {
        this.queryId = queryId;
    }

    @Override
    public void beforeRun() {
        SqlServiceImpl service = getService();
        // TODO [viliam]
//        service.getLiveOperationRegistry().register(this);
    }

    @Override
    public final void run() {
        CompletableFuture<?> future;
        try {
            future = doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
            return;
        }
        future.whenComplete(withTryCatch(getLogger(), (r, f) -> doSendResponse(f != null ? peel(f) : r)));
    }

    protected abstract CompletableFuture<?> doRun() throws Exception;

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    private void doSendResponse(Object value) {
        try {
            final SqlServiceImpl service = getService();
            // TODO [viliam]
//            service.getLiveOperationRegistry().deregister(this);
        } finally {
            sendResponse(value);
        }
    }

    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public final int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(queryId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        queryId = in.readObject();
    }
}
