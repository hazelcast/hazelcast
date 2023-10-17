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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ProcessorSupplier.Context;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

final class PlaceholderReplacer {

    private PlaceholderReplacer() {
    }

    static Document replacePlaceholders(Document doc, ExpressionEvalContext evalContext, JetSqlRow inputRow,
                                        String[] externalNames, boolean forRow) {
        Object[] values = inputRow.getValues();
        return replacePlaceholders(doc, evalContext, values, externalNames, forRow);
    }

    /**
     * Searches for nodes in Document that are strings matching pattern of {@code <!SomePlaceholder(params)!>}.
     *
     * <p>
     * Parameters are not known at query planning stage, they are
     * visible only in {@link ProcessorSupplier#init(Context)} method. That's why
     * we must use placeholders for the dynamic parameters, because we cannot transfer
     * {@linkplain org.apache.calcite.rex.RexNode} over the network, as it's not serializable.
     *
     * <p>
     * Similar restrictions are in the case of input references - input reference value is known only to the processor
     * receiving the input rows during job execution.
     *
     * @param readValueFromInput if true, then input reference is read from inputRow value, otherwise
     *                           it will use externalName to generate reference.
     */
    static Document replacePlaceholders(Document doc, ExpressionEvalContext evalContext,
                                        Object[] inputRow, String[] externalNames, boolean readValueFromInput) {
        Document result = new Document();
        for (Entry<String, Object> entry : doc.entrySet()) {
            String entryKey = entry.getKey();
            Object entryValue = entry.getValue();

            entryKey = (String) replace(entryKey, evalContext, inputRow, externalNames, true, readValueFromInput);
            if (entryValue instanceof String) {
               entryValue = replace((String) entryValue, evalContext, inputRow, externalNames, false, readValueFromInput);
            }

            if (entryValue instanceof List) {
                List<Object> newValues = new ArrayList<>();
                for (Object val : (List<?>) entryValue) {
                    Object v = val;
                   if (val instanceof Document) {
                       v = replacePlaceholders((Document) val, evalContext, inputRow, externalNames, readValueFromInput);
                   } else if (val instanceof String) {
                       v = replace((String) val, evalContext, inputRow, externalNames, false, readValueFromInput);
                   }
                   newValues.add(v);
                }
                entryValue = newValues;
            } else if (entryValue instanceof Document) {
                entryValue = replacePlaceholders((Document) entryValue, evalContext, inputRow, externalNames,
                        readValueFromInput);
            }

            result.append(entryKey, entryValue);
        }
        assert result.size() == doc.size() : "result size should match input size";
        return result;
    }

    static Object replace(String entryKey, ExpressionEvalContext evalContext, Object[] inputRow,
                          String[] externalNames, boolean key, boolean forRow) {
        DynamicParameter dynamicParameter = DynamicParameter.matches(entryKey);
        if (dynamicParameter != null) {
            Object arg = evalContext.getArgument(dynamicParameter.getIndex());
            assert !key || arg instanceof String : "keys must be Strings";
            return arg;
        }
        InputRef ref = InputRef.match(entryKey);
        if (ref != null) {
            if (key) {
                return externalNames[ref.getInputIndex()];
            } else if (!forRow) {
                return "$" + externalNames[ref.getInputIndex()];
            } else {
                return inputRow[ref.getInputIndex()];
            }
        }
        return entryKey;
    }
}
