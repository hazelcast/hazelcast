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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

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
    public InternalSerializationService getSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }
}
