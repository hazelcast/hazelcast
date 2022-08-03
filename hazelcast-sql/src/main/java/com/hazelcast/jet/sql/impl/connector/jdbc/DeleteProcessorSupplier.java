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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.DataSourceFromConnectionSupplier;
import com.hazelcast.jet.impl.connector.WriteJdbcP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.CommonDataSource;
import java.io.IOException;
import java.security.Permission;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

public class DeleteProcessorSupplier implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private String jdbcUrl;
    private String tableName;
    private List<String> pkFields;
    private List<String> fields;
    private String whereClause;
    private int batchLimit;

    @SuppressWarnings("unused")
    public DeleteProcessorSupplier() {
    }

    public DeleteProcessorSupplier(String jdbcUrl, String tableName, List<String> pkFields,
                                   List<String> fields, int batchLimit) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.pkFields = pkFields;
        this.fields = fields;
        this.batchLimit = batchLimit;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        whereClause = pkFields.stream().map(e -> e + " = ?")
                              .collect(joining(" AND "));
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        CommonDataSource ds = new DataSourceFromConnectionSupplier(jdbcUrl);
        for (int i = 0; i < count; i++) {
            Processor processor = new WriteJdbcP<>(
                    buildQuery(),
                    ds,
                    (PreparedStatement ps, JetSqlRow row) -> {
                        for (int j = 0; j < pkFields.size(); j++) {
                            int fieldIndex = fields.indexOf(pkFields.get(j));
                            ps.setObject(j + 1, row.get(fieldIndex));
                        }
                    },
                    false,
                    batchLimit
            );
            processors.add(processor);
        }
        return processors;
    }

    private String buildQuery() {
        return "DELETE FROM " + tableName +
                " WHERE " + whereClause;
    }

    @Nullable
    @Override
    public List<Permission> permissions() {
        return singletonList(ConnectorPermission.jdbc(jdbcUrl, ACTION_WRITE));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(jdbcUrl);
        out.writeString(tableName);
        writeStringList(out, pkFields);
        writeStringList(out, fields);
        out.writeInt(batchLimit);
    }

    private void writeStringList(ObjectDataOutput out, List<String> list) throws IOException {
        out.writeInt(list.size());
        for (int i = 0; i < list.size(); i++) {
            out.writeString(list.get(i));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jdbcUrl = in.readString();
        tableName = in.readString();
        pkFields = readStringList(in);
        fields = readStringList(in);
        batchLimit = in.readInt();
    }

    private List<String> readStringList(ObjectDataInput in) throws IOException {
        int numFields = in.readInt();
        List<String> fields = new ArrayList<>(numFields);
        for (int i = 0; i < numFields; i++) {
            fields.add(in.readString());
        }
        return fields;
    }
}
