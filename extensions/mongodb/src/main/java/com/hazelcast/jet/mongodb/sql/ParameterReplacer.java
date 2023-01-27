package com.hazelcast.jet.mongodb.sql;

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
     * Searches for nodes in Document with pattern like in {@linkplain #REPLACE_PARAM_PATTERN}
     * and replaces it with dynamic parameter.
     *
     * Not all parameters are known at query planning stage, some are
     * visible only in {@link com.hazelcast.jet.core.ProcessorSupplier#init(Context)} method. That's why
     * we must postpone the argument matching.
     * We cannot though transport {@linkplain org.apache.calcite.rex.RexNode} over the network, as it's not serializable,
     * so we are binding everythign we can in the connector and leave dynamic parameters for this method on PS side.
     */
    static Bson replacePlaceholders(Document doc, ExpressionEvalContext evalContext) {
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
                replacePlaceholders((Document) entry.getValue(), evalContext);
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
