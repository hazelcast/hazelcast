/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.util.List;

public final class SimpleExpressionEvalContext implements ExpressionEvalContext {

    public static final String SQL_ARGUMENTS_KEY_NAME = "__sql.arguments";

    private final List<Object> arguments;
    private final InternalSerializationService serializationService;

    private SimpleExpressionEvalContext(List<Object> arguments, InternalSerializationService serializationService) {
        this.arguments = arguments;
        this.serializationService = serializationService;
    }

    public static SimpleExpressionEvalContext from(ProcessorSupplier.Context ctx) {
        return new SimpleExpressionEvalContext(
                ctx.jobConfig().getArgument(SQL_ARGUMENTS_KEY_NAME),
                ((Contexts.ProcSupplierCtx) ctx).serializationService()
        );
    }

    public static SimpleExpressionEvalContext from(
            List<Object> arguments,
            InternalSerializationService serializationService
    ) {
        return new SimpleExpressionEvalContext(arguments, serializationService);
    }

    @Override
    public Object getArgument(int index) {
        return arguments.get(index);
    }

    @Override
    public List<Object> getArguments() {
        return arguments;
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return serializationService;
    }
}
