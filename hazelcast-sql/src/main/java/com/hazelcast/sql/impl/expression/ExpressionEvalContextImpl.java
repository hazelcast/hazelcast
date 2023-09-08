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
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Defines expression evaluation context contract for SQL {@link Expression
 * expressions}.
 *
 * @see Expression#eval
 */
public class ExpressionEvalContextImpl implements ExpressionEvalContext {

    private final List<Object> arguments;
    private final transient InternalSerializationService serializationService;
    private final transient NodeEngine nodeEngine;
    private final transient SqlSecurityContext securityContext;

    public ExpressionEvalContextImpl(
            @Nonnull List<Object> arguments,
            @Nonnull InternalSerializationService serializationService,
            @Nonnull NodeEngine nodeEngine) {
        this.arguments = requireNonNull(arguments);
        this.serializationService = requireNonNull(serializationService);
        this.nodeEngine = requireNonNull(nodeEngine);
        this.securityContext = NoOpSqlSecurityContext.INSTANCE;
    }

    public ExpressionEvalContextImpl(
            @Nonnull List<Object> arguments,
            @Nonnull InternalSerializationService serializationService,
            @Nonnull NodeEngine nodeEngine,
            @Nonnull SqlSecurityContext securityContext
    ) {
        this.arguments = requireNonNull(arguments);
        this.serializationService = requireNonNull(serializationService);
        this.nodeEngine = requireNonNull(nodeEngine);
        this.securityContext = securityContext;
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

    /**
     * @return node engine
     */
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * @return sql security context
     */
    public SqlSecurityContext getSecurityContext() {
        return securityContext;
    }
}
