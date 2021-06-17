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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.sql.impl.client.SqlClientUtils.expectedResultTypeToByte;
import static com.hazelcast.sql.impl.client.SqlClientUtils.expectedResultTypeToEnum;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SqlExecuteOperation extends SqlQueryOperation {

    private String sql;
    private List<Data> arguments;
    private long timeoutMillis;
    private int cursorBufferSize;
    private String schema;
    private SqlExpectedResultType expectedResultType;
    private Subject subject;

    public SqlExecuteOperation() {
    }

    public SqlExecuteOperation(
            QueryId queryId,
            String sql,
            List<Data> arguments,
            long timeoutMillis,
            int cursorBufferSize,
            String schema,
            SqlExpectedResultType expectedResultType,
            Subject subject
    ) {
        super(queryId);
        this.sql = sql;
        this.arguments = arguments;
        this.timeoutMillis = timeoutMillis;
        this.cursorBufferSize = cursorBufferSize;
        this.schema = schema;
        this.expectedResultType = expectedResultType;
        this.subject = subject;
    }

    @Override
    protected CompletableFuture<?> doRun() throws Exception {
        SqlSecurityContext sqlSecurityContext = prepareSecurityContext();
        SqlStatement query = new SqlStatement(sql);
        SerializationService serializationService = getNodeEngine().getSerializationService();
        for (Data arg : arguments) {
            query.addParameter(serializationService.toObject(arg));
        }
        query.setSchema(schema);
        query.setTimeoutMillis(timeoutMillis);
        query.setCursorBufferSize(cursorBufferSize);
        query.setExpectedResultType(expectedResultType);

        SqlServiceImpl sqlService = getNodeEngine().getSqlService();

        return completedFuture(sqlService.execute(query, sqlSecurityContext, getQueryId()));
    }

    private SqlSecurityContext prepareSecurityContext() {
        SecurityContext securityContext = ((NodeEngineImpl) getNodeEngine()).getNode().securityContext;

        if (securityContext == null) {
            return NoOpSqlSecurityContext.INSTANCE;
        } else {
            return securityContext.createSqlContext(subject);
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
        out.writeString(schema);
        out.writeByte(expectedResultTypeToByte(expectedResultType));
        out.writeObject(subject);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        sql = in.readString();
        arguments = in.readObject();
        timeoutMillis = in.readLong();
        cursorBufferSize = in.readInt();
        schema = in.readString();
        expectedResultType = expectedResultTypeToEnum(in.readByte());
        subject = in.readObject();
    }
}
