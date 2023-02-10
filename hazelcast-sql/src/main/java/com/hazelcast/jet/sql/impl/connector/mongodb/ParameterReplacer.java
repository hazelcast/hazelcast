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
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ParameterReplacer {
    private static final Pattern REPLACE_PARAM_PATTERN = Pattern.compile("/replaceParam:(\\d+)/");

    private ParameterReplacer() {
    }

    /**
     * Searches for nodes in Document with two properties: "objectType" = "DynamicParameter" and
     * index, that will be resolved as dynamic parameter index.
     *
     * Not all parameters are known at query planning stage, some are
     * visible only in {@link com.hazelcast.jet.core.ProcessorSupplier#init(Context)} method. That's why
     * we must postpone the argument matching.
     * We cannot though transport {@linkplain org.apache.calcite.rex.RexNode} over the network, as it's not serializable,
     * so we are binding everything we can in the connector and leave dynamic parameters for this method on PS side.
     */
    static Bson replacePlaceholders(Document doc, ExpressionEvalContext evalContext) {
        assert DynamicParameter.parse(doc) == null;
        for (Entry<String, Object> entry : doc.entrySet()) {
            if (entry.getValue() instanceof List) {
                List<?> value = (List<?>) entry.getValue();
                for (Object val : value) {
                   if (!(val instanceof Document)) {
                       throw new UnsupportedOperationException("idk what to do");
                   }

                   Document inner = (Document) val;
                   replacePlaceholders(inner, evalContext);
                }
            } else if (entry.getValue() instanceof String) {
                forString(evalContext, entry);
            } else if (entry.getValue() instanceof BsonString) {
                forBsonString(evalContext, entry);
            } else if (entry.getValue() instanceof Document) {
                Document value = (Document) entry.getValue();
                DynamicParameter param = DynamicParameter.parse(value);
                if (param != null) {
                    entry.setValue(evalContext.getArgument(param.getIndex()));
                } else {
                    replacePlaceholders(value, evalContext);
                }
            }
        }
        return doc;
    }

    private static void forBsonString(ExpressionEvalContext evalContext, Entry<String, Object> entry) {
        String string = ((BsonString) entry.getValue()).asString().toString();
        Matcher matcher = REPLACE_PARAM_PATTERN.matcher(string);
        if (matcher.find()) {
            int number = Integer.parseInt(matcher.group(1));
            Object argument = evalContext.getArgument(number);
            entry.setValue(argument);
        }
    }

    private static void forString(ExpressionEvalContext evalContext, Entry<String, Object> entry) {
        String string = entry.getValue().toString();
        Matcher matcher = REPLACE_PARAM_PATTERN.matcher(string);
        if (matcher.find()) {
            int number = Integer.parseInt(matcher.group(1));
            Object argument = evalContext.getArgument(number);
            entry.setValue(argument);
        }
    }

}
