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
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.security.Permission;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Defines expression evaluation context contract for SQL {@link Expression
 * expressions}.
 *
 * @see Expression#eval
 */
public class ExpressionEvalContextImpl implements ExpressionEvalContext {
    // Two security context holders for different scopes. If one is active, the other must be empty.
    // MetaSupplierCtx holds sql security context for Jet job execution.
    private transient MetaSupplierCtx contextRef;
    // SqlSecurityContext is applicable for non-Jet query execution (ByKey plans).
    private transient SqlSecurityContext ssc;

    private final List<Object> arguments;
    private final transient InternalSerializationService serializationService;
    private final transient NodeEngine nodeEngine;

    ExpressionEvalContextImpl(
            @Nonnull List<Object> arguments,
            @Nonnull InternalSerializationService serializationService,
            @Nonnull NodeEngine nodeEngine,
            @Nullable SqlSecurityContext ssc) {
        this.arguments = requireNonNull(arguments);
        this.serializationService = requireNonNull(serializationService);
        this.nodeEngine = requireNonNull(nodeEngine);
        this.ssc = ssc;
    }

    ExpressionEvalContextImpl(
            @Nonnull List<Object> arguments,
            @Nonnull InternalSerializationService serializationService,
            @Nonnull NodeEngine nodeEngine,
            @Nonnull MetaSupplierCtx context) {
        this.arguments = requireNonNull(arguments);
        this.serializationService = requireNonNull(serializationService);
        this.nodeEngine = requireNonNull(nodeEngine);
        this.contextRef = context;
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

    @Override
    public ExpressionEvalContextImpl withSerializationService(@Nonnull InternalSerializationService newService) {
        if (serializationService == newService) {
            return this;
        }
        if (contextRef != null) {
            return new ExpressionEvalContextImpl(arguments, newService, nodeEngine, contextRef);
        } else {
            return new ExpressionEvalContextImpl(arguments, newService, nodeEngine, ssc);
        }
    }

    /**
     * @return node engine
     */
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void checkPermission(Permission permission) {
        if (contextRef != null) {
            contextRef.checkPermission(permission);
        } else if (ssc != null) {
            ssc.checkPermission(permission);
        }
    }

    @Override
    @Nullable
    public Subject subject() {
        if (contextRef != null) {
            return contextRef.subject();
        }
        if (ssc != null) {
            return ssc.subject();
        }
        return null;
    }
}
