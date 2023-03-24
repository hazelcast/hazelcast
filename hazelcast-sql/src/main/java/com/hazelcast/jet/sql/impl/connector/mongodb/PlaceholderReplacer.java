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

    static Document replacePlaceholders(Document doc, ExpressionEvalContext evalContext, JetSqlRow inputRow) {
        Object[] values = inputRow.getValues();
        return replacePlaceholders(doc, evalContext, values);
    }

    /**
     * Searches for nodes in Document that are strings matching pattern of {@code <!SomePlaceholder(params)!>}.
     *
     * <p>
     * Not all parameters are known at query planning stage, some are
     * visible only in {@link com.hazelcast.jet.core.ProcessorSupplier#init(Context)} method. That's why
     * we must postpone the argument matching.
     * We cannot though transport {@linkplain org.apache.calcite.rex.RexNode} over the network, as it's not serializable,
     * so we are binding everything we can in the connector and leave dynamic parameters for this method on PS side.
     *
     * <p>
     * Similar restrictions are visible in case of input references - input reference value is known
     * during query execution.
     */
    static Document replacePlaceholders(Document doc, ExpressionEvalContext evalContext, Object[] inputRow) {
        Document result = new Document();
        for (Entry<String, Object> entry : doc.entrySet()) {
            String entryKey = entry.getKey();
            Object entryValue = entry.getValue();

            entryKey = replace(entryKey, evalContext, inputRow);
            if (entryValue instanceof String) {
               entryValue = replace((String) entryValue, evalContext, inputRow);
            }

            if (entryValue instanceof List) {
                List<Object> newValues = new ArrayList<>();
                for (Object val : (List<?>) entryValue) {
                    Object v = val;
                   if (val instanceof Document) {
                       v = replacePlaceholders((Document) val, evalContext, inputRow);
                   } else if (val instanceof String) {
                       v = replace((String) val, evalContext, inputRow);
                   }
                   newValues.add(v);
                }
                entryValue = newValues;
            } else if (entryValue instanceof Document) {
                entryValue = replacePlaceholders((Document) entryValue, evalContext, inputRow);
            }

            result.append(entryKey, entryValue);
        }
        assert result.size() == doc.size() : "result size should match input size";
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <T> T replace(T entryKey, ExpressionEvalContext evalContext, Object[] inputRow) {
        DynamicParameter dynamicParameter = DynamicParameter.matches(entryKey);
        if (dynamicParameter != null) {
            entryKey = (T) evalContext.getArgument(dynamicParameter.getIndex());
        }
        InputRef ref = InputRef.match(entryKey);
        if (ref != null) {
            entryKey = (T) inputRow[ref.getInputIndex()];
        }
        return entryKey;
    }
}
