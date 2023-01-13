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
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines expression evaluation context contract for SQL {@link Expression
 * expressions}.
 *
 * @see Expression#eval
 */
public class CorrelatedExpressionEvalContext extends ExpressionEvalContext {

    private final ExpressionEvalContext delegate;

    private final Map<Integer, JetSqlRow> correlationVariables;

    public CorrelatedExpressionEvalContext(
            @Nonnull ExpressionEvalContext delegate
    ) {
        //TODO: refactor to decorator pattern
        super(delegate.getArguments(), delegate.getSerializationService());
        this.delegate = delegate;
        this.correlationVariables = new HashMap<>();
    }

    /**
     * @param index argument index
     * @return the query argument
     */
    public Object getArgument(int index) {
        return delegate.getArgument(index);
    }

    /**
     * Return all the arguments.
     */
    public List<Object> getArguments() {
        return delegate.getArguments();
    }

    /**
     * @return serialization service
     */
    public InternalSerializationService getSerializationService() {
        return delegate.getSerializationService();
    }

    public void setCorrelationVariable(int i, JetSqlRow lefts) {
        correlationVariables.put(i, lefts);
    }

    public JetSqlRow getCorrelationVariable(int id) {
        return correlationVariables.get(id);
    }

    public Map<Integer, JetSqlRow> getCorrelationVariables() {
        return correlationVariables;
    }
}
