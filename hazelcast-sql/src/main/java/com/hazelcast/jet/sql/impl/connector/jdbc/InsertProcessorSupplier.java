/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.connector.WriteJdbcP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;
import static java.util.Collections.singletonList;

public class InsertProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements DataSerializable, SecuredFunction {

    private String query;
    private int batchLimit;

    @SuppressWarnings("unused")
    public InsertProcessorSupplier() {
    }

    public InsertProcessorSupplier(String externalDataStoreRef, String query, int batchLimit) {
        super(externalDataStoreRef);
        this.query = query;
        this.batchLimit = batchLimit;
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Processor processor = new WriteJdbcP<>(
                    query,
                    dataSource.get(),
                    (PreparedStatement ps, JetSqlRow row) -> {
                        for (int j = 0; j < row.getFieldCount(); j++) {
                            // JDBC parameterIndex is 1-based, so j + 1
                            ps.setObject(j + 1, row.get(j));
                        }
                    },
                    this::isNonTransientException,
                    false,
                    batchLimit
            );
            processors.add(processor);
        }
        return processors;
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private boolean isNonTransientException(SQLException e) {
        SQLException next = e.getNextException();
        return e instanceof SQLNonTransientException
                || e.getCause() instanceof SQLNonTransientException
                || !isTransientCode(e.getSQLState())
                || (next != null && e != next && isNonTransientException(next));
    }

    private boolean isTransientCode(String code) {
        // List of transient codes from:
        // https://github.com/npgsql/npgsql/blob/v7.0.0-preview.7/src/Npgsql/PostgresException.cs#L207-L227
        // Full list of error codes at:
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        switch (code) {
            case "53000": // insufficient_resources
            case "53100": // disk_full
            case "53200": // out_of_memory
            case "53300": // too_many_connections
            case "53400": // configuration_limit_exceeded
            case "57P03": // cannot_connect_now
            case "58000": // system_error
            case "58030": // io_error
            case "40001": // serialization_failure
            case "40P01": // deadlock_detected
            case "55P03": // lock_not_available
            case "55006": // object_in_use
            case "55000": // object_not_in_prerequisite_state
            case "08000": // connection_exception
            case "08003": // connection_does_not_exist
            case "08006": // connection_failure
            case "08001": // sqlclient_unable_to_establish_sqlconnection
            case "08004": // sqlserver_rejected_establishment_of_sqlconnection
            case "08007": // transaction_resolution_unknown
                return true;

            default:
                return false;
        }
    }

    @Nullable
    @Override
    public List<Permission> permissions() {
        return singletonList(ConnectorPermission.jdbc(externalDataStoreRef, ACTION_WRITE));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(externalDataStoreRef);
        out.writeString(query);
        out.writeInt(batchLimit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        externalDataStoreRef = in.readString();
        query = in.readString();
        batchLimit = in.readInt();
    }
}
