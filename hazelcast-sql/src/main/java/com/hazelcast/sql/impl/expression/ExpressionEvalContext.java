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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.ProcessorMetaSupplier.Context;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.security.Permission;
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
            Contexts.ProcSupplierCtx pCtx = (Contexts.ProcSupplierCtx) ctx;
            return new ExpressionEvalContextImpl(
                    requireNonNull(arguments),
                    pCtx.serializationService(),
                    pCtx.nodeEngine(),
                    pCtx
            );
        } else if (ctx instanceof Contexts.MetaSupplierCtx) {
            MetaSupplierCtx mCtx = (Contexts.MetaSupplierCtx) ctx;
            // Note that additional serializers configured for the job are not available in PMS.
            // Currently this is not needed.
            return new ExpressionEvalContextImpl(
                    arguments != null ? arguments : List.of(),
                    (InternalSerializationService) mCtx.nodeEngine().getSerializationService(),
                    mCtx.nodeEngine(),
                    mCtx
            );
        } else {
            // Path intended for test code
            assert ctx instanceof TestProcessorMetaSupplierContext;
            if (arguments == null) {
                arguments = new ArrayList<>();
            }
            return new ExpressionEvalContextImpl(
                    arguments,
                    new DefaultSerializationServiceBuilder().build(),
                    Util.getNodeEngine(ctx.hazelcastInstance()),
                    NoOpSqlSecurityContext.INSTANCE
            );
        }
    }

    static ExpressionEvalContext createContext(
            @Nonnull List<Object> arguments,
            @Nonnull NodeEngine nodeEngine,
            @Nonnull InternalSerializationService iss,
            @Nullable SqlSecurityContext ssc) {
        return new ExpressionEvalContextImpl(arguments, iss, nodeEngine, ssc);
    }

    static ExpressionEvalContext createContext(
            @Nonnull List<Object> arguments,
            @Nonnull HazelcastInstance hz,
            @Nonnull InternalSerializationService iss,
            @Nullable SqlSecurityContext ssc) {
        return createContext(arguments, Util.getNodeEngine(hz), iss, ssc);
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
     * Changes serialization service for this context
     * @return context with changed serialization service
     */
    ExpressionEvalContext withSerializationService(@Nonnull InternalSerializationService newService);

    /**
     * @return node engine
     */
    NodeEngine getNodeEngine();

    /**
     * checks security permissions
     */
    void checkPermission(Permission permission);

    @Nullable
    Subject subject();
}
