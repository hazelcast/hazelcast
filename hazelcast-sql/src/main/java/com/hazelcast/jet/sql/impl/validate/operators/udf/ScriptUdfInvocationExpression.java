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

package com.hazelcast.jet.sql.impl.validate.operators.udf;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.management.ScriptEngineManagerContext;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.schema.TablesStorage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.schema.function.UserDefinedFunction;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.util.Arrays;

class ScriptUdfInvocationExpression extends VariExpression<Object> {
    private String name;
    private QueryDataType returnType;

    ScriptUdfInvocationExpression(String name, QueryDataType returnType, Expression<?>[] operands) {
        super(operands);
        this.name = name;
        this.returnType = returnType;
    }

    @Override
    public Object eval(Row row, ExpressionEvalContext context) {
        Object[] parameterValues = Arrays.stream(operands())
                .map(ex -> ExpressionUtil.evaluate(ex, row, context))
                .toArray();

        TablesStorage ts = new TablesStorage(context.getNodeEngine());
        UserDefinedFunction definition = ts.getFunction(name);

        ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();
        ScriptEngine scriptEngine = scriptEngineManager.getEngineByName(definition.getLanguage());
        if (scriptEngine == null) {
            throw new IllegalArgumentException("Could not find ScriptEngine named '" + definition.getLanguage() + "'."
                    + " Please add the corresponding ScriptEngine to the classpath of this Hazelcast member");
        }
        scriptEngine.put("hazelcast", context.getNodeEngine().getHazelcastInstance());
        scriptEngine.put("sql", context.getNodeEngine().getHazelcastInstance().getSql());
        for (int i = 0; i < definition.getParameterNames().size(); ++i) {
            // TODO: convert types?
            scriptEngine.put(definition.getParameterNames().get(i), parameterValues[i]);
        }
        try {
            Object rawResult = scriptEngine.eval(definition.getBody());
            return definition.getReturnType().convert(rawResult);
        } catch (ScriptException e) {
            // ScriptException's cause is not serializable - we don't need the cause
            HazelcastException hazelcastException = new HazelcastException(e.getMessage());
            hazelcastException.setStackTrace(e.getStackTrace());
            throw hazelcastException;
        }
    }

    @Override
    public boolean isCooperative() {
        // script functions are not invoked in asynch way
        return false;
    }

    @Override
    public QueryDataType getType() {
        return returnType;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeString(name);
        out.writeObject(returnType);
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeUTF(name);
        out.writeObject(returnType);
    }
    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        name = in.readUTF();
        returnType = (QueryDataType) in.readObject();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        name = in.readString();
        returnType = in.readObject();
    }
}
