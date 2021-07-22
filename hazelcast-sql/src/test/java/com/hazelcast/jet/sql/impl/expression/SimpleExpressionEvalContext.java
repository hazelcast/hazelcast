/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.util.Arrays;
import java.util.List;

public final class SimpleExpressionEvalContext implements ExpressionEvalContext {

    private final List<Object> args;

    public static SimpleExpressionEvalContext create(Object... args) {
        if (args == null) {
            args = new Object[0];
        }

        return new SimpleExpressionEvalContext(Arrays.asList(args));
    }

    private SimpleExpressionEvalContext(List<Object> args) {
        this.args = args;
    }

    @Override
    public Object getArgument(int index) {
        return args.get(index);
    }

    @Override
    public List<Object> getArguments() {
        return args;
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }
}
