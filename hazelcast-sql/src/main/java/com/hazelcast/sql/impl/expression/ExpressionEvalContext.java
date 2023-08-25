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
import com.hazelcast.jet.core.ProcessorMetaSupplier.Context;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static java.util.Objects.requireNonNull;

/**
 * Defines expression evaluation context contract for SQL {@link Expression expressions}.
 *
 * @see Expression#eval
 */
public interface ExpressionEvalContext {

    static ExpressionEvalContext from(Context ctx) {
        List<Object> arguments = ctx.jobConfig().getArgument(SQL_ARGUMENTS_KEY_NAME);
        if (ctx instanceof Contexts.ProcSupplierCtx) {
            return new ExpressionEvalContextImpl(
                    requireNonNull(arguments),
                    ((Contexts.ProcSupplierCtx) ctx).serializationService(),
                    ((Contexts.ProcSupplierCtx) ctx).nodeEngine());
        } else {
            if (arguments == null) {
                arguments = new ArrayList<>();
            }
            return new ExpressionEvalContextImpl(
                    arguments,
                    new DefaultSerializationServiceBuilder().build(),
                    Util.getNodeEngine(ctx.hazelcastInstance()));
        }
    }

    /**
     * @param index argument index
     * @return the query argument
     */
    Object getArgument(int index);

    /**
     * Return all the arguments.
     */
    List<Object> getArguments();

    /**
     * @return serialization service
     */
    InternalSerializationService getSerializationService();

    /**
     * @return node engine
     */
    NodeEngine getNodeEngine();
}
