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

import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

class UdfInvocationExpression extends VariExpression<Object> {
    private Method eval;

    UdfInvocationExpression(Method eval, Expression<?>[] operands) {
        super(operands);
        this.eval = eval;
    }

    @Override
    public Object eval(Row row, ExpressionEvalContext context) {
        try {
            return eval.invoke(null, Arrays.stream(operands())
                    .map(ex -> ExpressionUtil.evaluate(ex, row, context))
                    .toArray());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public QueryDataType getType() {
        // TODO: infer from signature. This is different type metadata than for return type
        return QueryDataType.VARCHAR;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeString(eval.getDeclaringClass().getName());
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeUTF(eval.getDeclaringClass().getName());
    }
    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        String funcClass = in.readUTF();
        ScalarUserDefinedFunctionDefinition funcDef = ReflectionUtils.newInstance(null, funcClass);
        eval = funcDef.getEvalMethod();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        String funcClass = in.readString();
        ScalarUserDefinedFunctionDefinition funcDef = ReflectionUtils.newInstance(null, funcClass);
        eval = funcDef.getEvalMethod();
    }
}
