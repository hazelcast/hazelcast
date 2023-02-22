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
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> getArguments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return ss;
    }

    @Override
    public NodeEngine getNodeEngine() {
        throw new UnsupportedOperationException();
    }
}
