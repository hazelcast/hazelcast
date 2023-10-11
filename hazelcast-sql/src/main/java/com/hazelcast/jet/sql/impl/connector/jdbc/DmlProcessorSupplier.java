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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.WriteJdbcP;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DmlProcessorSupplier
        extends AbstractJdbcSqlConnectorProcessorSupplier
        implements ProcessorSupplier, DataSerializable, SecuredFunction {

    private String query;
    private int[] dynamicParams;
    private int[] inputRefs;
    private int batchLimit;

    private transient ExpressionEvalContext evalContext;

    @SuppressWarnings("unused")
    public DmlProcessorSupplier() {
    }

    /**
     * @param dynamicParams An array specifying what argument to bind as i-th
     *                      parameter value.
     * @param inputRefs     An array specifying what input reference to bind as
     *                      j-th parameter value, starting at i-th index equal to dynamicParams size
     */
    public DmlProcessorSupplier(
            @Nonnull String dataConnectionName,
            @Nonnull String query,
            @Nonnull int[] dynamicParams,
            @Nonnull int[] inputRefs,
            int batchLimit
    ) {
        super(dataConnectionName);
        this.query = requireNonNull(query, "query must not be null");
        this.dynamicParams = requireNonNull(dynamicParams, "dynamicParams must not be null");
        this.inputRefs = requireNonNull(inputRefs, "inputRefs must not be null");
        this.batchLimit = batchLimit;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        evalContext = ExpressionEvalContext.from(context);
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
                        List<Object> arguments = evalContext.getArguments();

                        for (int j = 0; j < dynamicParams.length; j++) {
                            Object v = arguments.get(dynamicParams[j]);
                            ps.setObject(j + 1, v);
                        }

                        for (int j = 0; j < inputRefs.length; j++) {
                            Object v = row.get(inputRefs[j]);
                            ps.setObject(dynamicParams.length + j + 1, v);
                        }

                    },
                    false,
                    batchLimit
            );
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(dataConnectionName);
        out.writeString(query);
        out.writeIntArray(dynamicParams);
        out.writeIntArray(inputRefs);
        out.writeInt(batchLimit);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        dataConnectionName = in.readString();
        query = in.readString();
        dynamicParams = in.readIntArray();
        inputRefs = in.readIntArray();
        batchLimit = in.readInt();
    }
}
