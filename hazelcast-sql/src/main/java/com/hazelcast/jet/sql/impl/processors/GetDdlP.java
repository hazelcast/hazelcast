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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;

public class GetDdlP extends AbstractProcessor {
    private transient InternalSerializationService ss;
    private String ddl;

    public GetDdlP(String ddl) {
        this.ddl = ddl;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        ExpressionEvalContext evalContext = ExpressionEvalContext.from(context);
        ss = evalContext.getSerializationService();
        super.init(context);
    }

    @Override
    public boolean complete() {
        return tryEmit(new JetSqlRow(ss, new String[]{ddl}));
    }

    public static GetDdlProcessorSupplier getDdlSupplier(String ddl) {
        return new GetDdlProcessorSupplier(ddl);
    }

    static final class GetDdlProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private String ddl;

        @SuppressWarnings("unused")
        private GetDdlProcessorSupplier() {
        }

        private GetDdlProcessorSupplier(@Nonnull final String ddl) {
            this.ddl = ddl;
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            return singletonList(new GetDdlP(ddl));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(ddl);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            ddl = in.readString();
        }
    }
}