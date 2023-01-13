/*
 * Copyright 2023 Hazelcast Inc.
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
                    dataSource,
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
        // Full list of error codes at:
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        switch (code) {
            // Sorted alphabetically
            case "08000":
            case "08001":
            case "08003":
            case "08004":
            case "08006":
            case "08007":
            case "40001":
            case "40P01":
            case "53000":
            case "53100":
            case "53200":
            case "53300":
            case "53400":
            case "55000":
            case "55006":
            case "55P03":
            case "57P03":
            case "58000":
            case "58030":
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
