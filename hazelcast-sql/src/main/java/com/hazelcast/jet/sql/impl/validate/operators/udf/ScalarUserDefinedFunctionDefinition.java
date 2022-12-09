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

package com.hazelcast.jet.sql.impl.validate.operators.udf;

import com.hazelcast.internal.util.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public abstract class ScalarUserDefinedFunctionDefinition {

    protected Method getEvalMethod() {
        List<Method> methods = Arrays.stream(getClass().getMethods())
                .filter(m -> m.getName().equals("eval"))
                .collect(Collectors.toList());

        Preconditions.checkFalse(methods.isEmpty(), "No eval method");
        // TODO: maybe function can be overloaded?
        Preconditions.checkFalse(methods.size() > 1, "Ambiguous eval method");
        Preconditions.checkTrue(Modifier.isStatic(methods.get(0).getModifiers()), "Eval must be static");

        return methods.get(0);
    }

    public String getName() {
        return getClass().getSimpleName();
    }

    public SqlTypeName[] argumentTypes() {
        // TODO: use types from eval method signature
        return new SqlTypeName[]{VARCHAR};
    }

    // TODO: use return type from eval method signature
    public SqlTypeName returnType() {
        return VARCHAR;
    }

    // TODO: example metadata for optimizer
    public boolean isDeterministic() {
        return true;
    }
}
