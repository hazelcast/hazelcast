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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import com.hazelcast.jet.impl.execution.init.Contexts;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static java.util.Objects.requireNonNull;

/**
 * Defines expression evaluation context contract for SQL {@link Expression
 * expressions}.
 *
 * @see Expression#eval
 */
public class ExpressionEvalContext {

    private final List<Object> arguments;
    private final InternalSerializationService serializationService;

    public ExpressionEvalContext(
            @Nonnull List<Object> arguments,
            @Nonnull InternalSerializationService serializationService
    ) {
        this.arguments = requireNonNull(arguments);
        this.serializationService = requireNonNull(serializationService);
    }

    public static ExpressionEvalContext from(ProcessorSupplier.Context ctx) {
        List<Object> arguments = ctx.jobConfig().getArgument(SQL_ARGUMENTS_KEY_NAME);
        if (ctx instanceof TestProcessorSupplierContext) {
            if (arguments == null) {
                arguments = new ArrayList<>();
            }
            return new ExpressionEvalContext(arguments, new DefaultSerializationServiceBuilder().build());
        } else {
            return new ExpressionEvalContext(
                    requireNonNull(arguments),
                    ((Contexts.ProcSupplierCtx) ctx).serializationService());
        }
    }

    /**
     * @param index argument index
     * @return the query argument
     */
    public Object getArgument(int index) {
        return arguments.get(index);
    }

    /**
     * Return all the arguments.
     */
    public List<Object> getArguments() {
        return arguments;
    }

    /**
     * @return serialization service
     */
    public InternalSerializationService getSerializationService() {
        return serializationService;
    }
}
