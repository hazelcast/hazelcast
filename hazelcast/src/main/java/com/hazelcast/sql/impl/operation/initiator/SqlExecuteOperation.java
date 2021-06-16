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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.client.SqlClientUtils;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.sql.impl.client.SqlClientUtils.expectedResultTypeToByte;
import static com.hazelcast.sql.impl.client.SqlClientUtils.expectedResultTypeToEnum;

public class SqlExecuteOperation extends SqlOperation {
    private String sql;
    private List<Object> arguments;
    private long timeoutMillis;
    private int cursorBufferSize;
    private SqlExpectedResultType expectedResultType;
    private QueryId queryId;

    public SqlExecuteOperation() {
    }

    public SqlExecuteOperation(
            String sql,
            List<Object> arguments,
            long timeoutMillis,
            int cursorBufferSize,
            SqlExpectedResultType expectedResultType,
            QueryId queryId
    ) {
        this.sql = sql;
        this.arguments = arguments;
        this.timeoutMillis = timeoutMillis;
        this.cursorBufferSize = cursorBufferSize;
        this.expectedResultType = expectedResultType;
        this.queryId = queryId;
    }

    @Override
    protected CompletableFuture<?> doRun() throws Exception {
        SqlSecurityContext sqlSecurityContext = prepareSecurityContext();

        SqlStatement query = new SqlStatement(parameters.sql);

        for (Data param : parameters.parameters) {
            query.addParameter(serializationService.toObject(param));
        }

        query.setSchema(parameters.schema);
        query.setTimeoutMillis(parameters.timeoutMillis);
        query.setCursorBufferSize(parameters.cursorBufferSize);
        query.setExpectedResultType(SqlClientUtils.expectedResultTypeToEnum(parameters.expectedResultType));

        SqlServiceImpl sqlService = nodeEngine.getSqlService();

        // TODO [viliam] make sure the light job is optimized locally
        return sqlService.execute(query, sqlSecurityContext, parameters.queryId);
    }

    private SqlSecurityContext prepareSecurityContext() {
        SecurityContext securityContext = clientEngine.getSecurityContext();

        if (securityContext == null) {
            return NoOpSqlSecurityContext.INSTANCE;
        } else {
            return securityContext.createSqlContext(endpoint.getSubject());
        }
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.OPERATION_EXECUTE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(sql);
        out.writeObject(arguments);
        out.writeLong(timeoutMillis);
        out.writeInt(cursorBufferSize);
        out.writeByte(expectedResultTypeToByte(expectedResultType));
        out.writeObject(queryId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        sql = in.readString();
        arguments = in.readObject();
        timeoutMillis = in.readLong();
        cursorBufferSize = in.readInt();
        expectedResultType = expectedResultTypeToEnum(in.readByte());
        queryId = in.readObject();
    }
}
