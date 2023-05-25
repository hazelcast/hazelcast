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

import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.jet.mongodb.impl.Mappers.bsonToDocument;

/**
 * A placeholder that will be replaced during query execution.
 */
public interface DynamicallyReplacedPlaceholder extends Serializable {

    /**
     * Representation of this placeholder as a string.
     */
    @Nonnull
    String asString();

    static Document replacePlaceholdersInPredicate(Object predicate, String[] externalNames,
                                                   ExpressionEvalContext evalContext) {
        if (predicate instanceof String) {
            InputRef ref = InputRef.match(predicate);
            if (ref != null) {
                int index = ref.getInputIndex();
                String colName = externalNames[index];
                return bsonToDocument(Aggregates.match(Filters.eq(colName, true)));
            } else {
                throw new UnsupportedOperationException("unknown predicate " + predicate);
            }
        } else if (predicate instanceof Document) {
            return PlaceholderReplacer.replacePlaceholders((Document) predicate, evalContext, (Object[]) null,
                    externalNames, false);
        }
        assert predicate instanceof Bson;
        return bsonToDocument((Bson) predicate);
    }

}
