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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.security.Permission;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Specifies an untrusted context for assessing expressions.
 * Contains identical information as the trusted context {@code ExpressionEvalContextImpl}, except for the security context.
 * If an attempt is made to acquire a security context, an exception will be raised,
 * indicating that the context lacks trust.
 * This class is utilized in scenarios where obtaining a trusted security context is unfeasible.
 */
public class UntrustedExpressionEvalContext
        implements ExpressionEvalContext {
    private final List<Object> arguments;
    private final transient InternalSerializationService serializationService;
    private final transient NodeEngine nodeEngine;

    public UntrustedExpressionEvalContext(
            @Nonnull List<Object> arguments,
            @Nonnull InternalSerializationService serializationService,
            @Nonnull NodeEngine nodeEngine) {
        this.arguments = requireNonNull(arguments);
        this.serializationService = requireNonNull(serializationService);
        this.nodeEngine = requireNonNull(nodeEngine);
    }

    public static UntrustedExpressionEvalContext from(ExpressionEvalContext context) {
        return new UntrustedExpressionEvalContext(
                context.getArguments(),
                context.getSerializationService(),
                context.getNodeEngine()
        );
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

    @Override
    public UntrustedExpressionEvalContext withSerializationService(@Nonnull InternalSerializationService newService) {
        if (serializationService == newService) {
            return this;
        }
        return new UntrustedExpressionEvalContext(
                arguments,
                newService,
                nodeEngine
        );
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void checkPermission(Permission permission) {
        if (nodeEngine.getConfig().getSecurityConfig().isEnabled()) {
            throw new SecurityException("Unable to employ sensitive functions in untrusted invocations.");
        }
    }

    @Override
    @Nullable
    public Subject subject() {
        throw new SecurityException("Unable to employ sensitive functions in untrusted invocations.");
    }
}
