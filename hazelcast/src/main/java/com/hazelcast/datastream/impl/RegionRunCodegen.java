/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.BetweenPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Set;

public abstract class RegionRunCodegen {

    protected final Predicate query;
    protected final RecordModel recordModel;
    protected final String compilationId;
    protected final QueryAnalyzer queryAnalyzer;
    private final StringBuffer codeBuffer = new StringBuffer();
    private final Set<String> parameters;
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final Set<String> attributes;

    public RegionRunCodegen(String compilationId, Predicate query, RecordModel recordModel) {
        this.compilationId = compilationId;
        this.query = query;
        this.recordModel = recordModel;
        this.queryAnalyzer = new QueryAnalyzer(query, recordModel);
        this.parameters = queryAnalyzer.getParameters();
        this.attributes = queryAnalyzer.getAttributes();
    }

    public abstract String className();

    public abstract void generate();

    protected void addParamFields() {
        if (parameters.size() > 0) {
            for (String variable : parameters) {
                Field variableField = recordModel.getField(variable);
                add("    private %s param_%s;\n", variableField.getType().getName(), variable);
            }
            add("\n");
        }
    }

    public RegionRunCodegen add(String s, Object... args) {
        codeBuffer.append(String.format(s, (Object[]) args));
        return this;
    }

    public RegionRunCodegen add(long i) {
        codeBuffer.append(i);
        return this;
    }

    protected void addBindMethod() {
        add("    public void bind(Map<String, Object> binding){\n");
        for (String variable : parameters) {
            Field field = recordModel.getField(variable);
            add("        param_%s=", variable);
            add("(");
            if (field.getType().equals(Integer.TYPE)) {
                add("Integer");
            } else if (field.getType().equals(Long.TYPE)) {
                add("Long");
            } else if (field.getType().equals(Short.TYPE)) {
                add("Short");
            } else if (field.getType().equals(Float.TYPE)) {
                add("Float");
            } else if (field.getType().equals(Double.TYPE)) {
                add("Double");
            } else if (field.getType().equals(Boolean.TYPE)) {
                add("Boolean");
            } else if (field.getType().equals(Short.TYPE)) {
                add("Short");
            } else if (field.getType().equals(Byte.TYPE)) {
                add("Byte");
            } else {
                throw new RuntimeException();
            }
            add(")binding.get(\"").add(variable).add("\");\n");
        }
        add("    }\n\n");
    }

    public String getCode() {
        return codeBuffer.toString();
    }

    protected void toCode(Predicate predicate, int unrollIndex) {
        if (predicate instanceof SqlPredicate) {
            toCode((SqlPredicate) predicate, unrollIndex);
        } else if (predicate instanceof TruePredicate) {
            add(" true ");
        } else if (predicate instanceof NotPredicate) {
            toCode((NotPredicate) predicate, unrollIndex);
        } else if (predicate instanceof AndPredicate) {
            toCode((AndPredicate) predicate, unrollIndex);
        } else if (predicate instanceof OrPredicate) {
            toCode((OrPredicate) predicate, unrollIndex);
        } else if (predicate instanceof BetweenPredicate) {
            toCode((BetweenPredicate) predicate, unrollIndex);
        } else if (predicate instanceof NotEqualPredicate) {
            toCode((NotEqualPredicate) predicate, unrollIndex);
        } else if (predicate instanceof EqualPredicate) {
            toCode((EqualPredicate) predicate, unrollIndex);
        } else if (predicate instanceof GreaterLessPredicate) {
            toCode((GreaterLessPredicate) predicate, unrollIndex);
        } else {
            throw new RuntimeException("Unhandled predicate:" + predicate.getClass());
        }
    }

    private void toCode(GreaterLessPredicate predicate, int unrollIndex) {
        String operator;
        if (predicate.isEqual()) {
            if (predicate.isLess()) {
                operator = "<=";
            } else {
                operator = ">=";
            }
        } else {
            if (predicate.isLess()) {
                operator = "<";
            } else {
                operator = ">";
            }
        }

        comparisonToCode(predicate.getAttributeName(), predicate.getValue(), operator, unrollIndex);
    }

    private void toCode(EqualPredicate predicate, int unrollIndex) {
        String attributeName = predicate.getAttributeName();
        Comparable value = predicate.getValue();
        if (attributeName.equals("true") && value.equals("true")) {
            add(" true ");
        } else {
            comparisonToCode(attributeName, value, "==", unrollIndex);
        }
    }

    private void toCode(NotEqualPredicate predicate, int unrollIndex) {
        comparisonToCode(predicate.getAttributeName(), predicate.getValue(), "!=", unrollIndex);
    }

    private void toCode(BetweenPredicate predicate, int unrollIndex) {
        // between predicate is rewritten to:((attribute>=from) and (attribute<=to))
        GreaterLessPredicate left = new GreaterLessPredicate(
                predicate.getAttributeName(), predicate.getFrom(), true, false);
        GreaterLessPredicate right = new GreaterLessPredicate(
                predicate.getAttributeName(), predicate.getTo(), true, true);
        AndPredicate andPredicate = new AndPredicate(left, right);
        toCode((Predicate) andPredicate, unrollIndex);
    }

    private void toCode(OrPredicate predicate, int unrollIndex) {
        boolean first = true;
        for (Predicate p : predicate.getPredicates()) {
            if (first) {
                first = false;
            } else {
                add(" || ");
            }
            add("(");
            toCode(p, unrollIndex);
            add(")");
        }
    }

    private void toCode(AndPredicate predicate, int unrollIndex) {
        boolean first = true;
        for (Predicate p : predicate.getPredicates()) {
            if (first) {
                first = false;
            } else {
                add(" && ");
            }
            add("(");
            toCode(p, unrollIndex);
            add(")");
        }
    }

    private void toCode(NotPredicate predicate, int unrollIndex) {
        add(" !(");
        toCode((Predicate) predicate, unrollIndex);
        add(")");
    }

    private void toCode(SqlPredicate predicate, int unrollIndex) {
        toCode(predicate.getPredicate(), unrollIndex);
    }

    private void comparisonToCode(String attributeName, Comparable value, String operator, int unrollIndex) {
        addGetField(attributeName, unrollIndex);
        add(operator);

        if (value instanceof String) {
            String valueString = (String) value;
            if (valueString.startsWith("$")) {
                String variableName = valueString.substring(1);
                add("param_%s", variableName);
            } else {
                add(valueString);
            }
        } else {
            add(value.toString());
        }
    }

    protected void addGetField(String attributeName) {
        addGetField(attributeName, 0);
    }

    protected void addGetField(String attributeName, int unrollIndex) {
        Field field = recordModel.getField(attributeName);
        long offset = unsafe.objectFieldOffset(field) - recordModel.getDataOffset();

        if (field.getType().equals(Integer.TYPE)) {
            add("unsafe.getInt(");
        } else if (field.getType().equals(Long.TYPE)) {
            add("unsafe.getLong(");
        } else if (field.getType().equals(Short.TYPE)) {
            add("unsafe.getShort(");
        } else if (field.getType().equals(Float.TYPE)) {
            add("unsafe.getFloat(");
        } else if (field.getType().equals(Double.TYPE)) {
            add("unsafe.getDouble(");
        } else if (field.getType().equals(Boolean.TYPE)) {
            add("unsafe.getBoolean(null,");
        } else if (field.getType().equals(Short.TYPE)) {
            add("unsafe.getShort(");
        } else if (field.getType().equals(Character.TYPE)) {
            add("unsafe.getChar(");
        } else {
            throw new RuntimeException("Unhandled field comparison: '" + field.getType() + "' for attribute:" + attributeName);
        }

        add("recordAddress");

        if (unrollIndex > 0) {
            add("+" + (recordModel.getSize() * unrollIndex));
        }
        add("+").add(offset);
        add(")");
    }

    protected void addPutField(String attributeName, int unrollIndex, String valueVariableName) {
        Field field = recordModel.getField(attributeName);
        long offset = unsafe.objectFieldOffset(field) - recordModel.getDataOffset();

        if (field.getType().equals(Integer.TYPE)) {
            add("unsafe.putInt(");
        } else if (field.getType().equals(Long.TYPE)) {
            add("unsafe.putLong(");
        } else if (field.getType().equals(Short.TYPE)) {
            add("unsafe.putShort(");
        } else if (field.getType().equals(Float.TYPE)) {
            add("unsafe.putFloat(");
        } else if (field.getType().equals(Double.TYPE)) {
            add("unsafe.putDouble(");
        } else if (field.getType().equals(Boolean.TYPE)) {
            add("unsafe.putBoolean(null,");
        } else if (field.getType().equals(Short.TYPE)) {
            add("unsafe.putShort(");
        } else if (field.getType().equals(Character.TYPE)) {
            add("unsafe.putChar(");
        } else {
            throw new RuntimeException("Unhandled field comparison: '" + field.getType() + "' for attribute:" + attributeName);
        }

        add("recordAddress");

        if (unrollIndex > 0) {
            add("+" + (recordModel.getSize() * unrollIndex));
        }
        add("+").add(offset);
        add("," + valueVariableName);
        add(")");
    }
}
