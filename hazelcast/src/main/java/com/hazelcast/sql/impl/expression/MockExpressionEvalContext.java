/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.NodeEngine;

import java.util.List;

public class MockExpressionEvalContext implements ExpressionEvalContext {
    InternalSerializationService ss;

    public MockExpressionEvalContext() {
        this.ss = new DefaultSerializationServiceBuilder().build();
    }

    @Override
    public Object getArgument(int index) {
        throw new UnsupportedOperationException("getArgument operation is not supported for Mock EEC");
    }

    @Override
    public List<Object> getArguments() {
        throw new UnsupportedOperationException("getArgument operation is not supported for Mock EEC");
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return ss;
    }

    @Override
    public NodeEngine getNodeEngine() {
        throw new UnsupportedOperationException("getNodeEngine operation is not supported for Mock EEC");
    }
}
