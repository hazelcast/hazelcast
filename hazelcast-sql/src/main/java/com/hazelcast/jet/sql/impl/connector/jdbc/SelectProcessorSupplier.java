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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.ReadJdbcP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("checkstyle:ExecutableStatementCount")
public class SelectProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private String query;
    private int[] parameterPositions;
    private List<FunctionEx<Object, ?>> converters = new ArrayList<>();

    private transient ExpressionEvalContext evalContext;
    private transient volatile BiFunctionEx<ResultSet, Integer, ?>[] valueGetters;
    private String dialectName;

    @SuppressWarnings("unused")
    public SelectProcessorSupplier() {
    }

    public SelectProcessorSupplier(@Nonnull String dataConnectionName,
                                   @Nonnull String query,
                                   @Nonnull int[] parameterPositions,
                                   @Nonnull List<FunctionEx<Object, ?>> converters,
                                   @Nonnull String dialectName) {
        super(dataConnectionName);
        this.query = requireNonNull(query, "query must not be null");
        this.parameterPositions = requireNonNull(parameterPositions, "parameterPositions must not be null");
        this.converters = converters;
        this.dialectName = dialectName;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        this.evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        assert count == 1;

        Processor processor = new ReadJdbcP<>(
                () -> dataConnection.getConnection(),
                (connection, parallelism, index) -> {
                    PreparedStatement statement = connection.prepareStatement(query);
                    List<Object> arguments = evalContext.getArguments();
                    for (int j = 0; j < parameterPositions.length; j++) {
                        statement.setObject(j + 1, arguments.get(parameterPositions[j]));
                    }
                    try {
                        ResultSet rs = statement.executeQuery();
                        valueGetters = prepareValueGettersFromMetadata(rs);
                        return rs;
                    } catch (SQLException e) {
                        statement.close();
                        throw e;
                    }
                },
                rs -> {
                    int columnCount = rs.getMetaData().getColumnCount();
                    Object[] row = new Object[columnCount];
                    for (int j = 0; j < columnCount; j++) {
                        Object value = valueGetters[j].apply(rs, j + 1);
                        row[j] = value;
                    }

                    return new JetSqlRow(evalContext.getSerializationService(), row);
                }
        );

        return singleton(processor);
    }

    private BiFunctionEx<ResultSet, Integer, ?>[] prepareValueGettersFromMetadata(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();

        BiFunctionEx<ResultSet, Integer, Object>[] valueGetters = new BiFunctionEx[metaData.getColumnCount()];
        for (int j = 0; j < metaData.getColumnCount(); j++) {
            String type = metaData.getColumnTypeName(j + 1).toUpperCase(Locale.ROOT);
            TriFunction<ResultSetMetaData, String, Integer, String> typeCheck;
            typeCheck = DatabaseSpecificTypeCheck.getTypeCheck(dialectName);
            type = typeCheck.apply(metaData, type, j);

            Map<String, BiFunctionEx<ResultSet, Integer, ?>> getters = GettersProvider.getGetters(dialectName);
            FunctionEx<? super Object, ?> converterFn = converters.get(j);
            valueGetters[j] = getters.getOrDefault(
                    type,
                    (resultSet, n) -> rs.getObject(n)
            ).andThen(converterFn);
        }
        return valueGetters;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnectionName);
        out.writeString(query);
        out.writeIntArray(parameterPositions);
        out.writeObject(converters);
        out.writeString(dialectName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        parameterPositions = in.readIntArray();
        converters = in.readObject();
        dialectName = in.readString();
    }

}
