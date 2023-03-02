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
package com.hazelcast.jet.mongodb.impl;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.unset;
import static java.util.Collections.singletonList;

final class MongoUtilities {

    private MongoUtilities() {
    }

    /**
     * Adds an aggregate to the query, that for each document will calculate
     * {@code document._id.getTimestamp().getTime() MOD totalParallelism == processorIndex}, using MongoDB
     * <a href="https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/">function aggregate</a>.
     * <p>
     * Only if this comparison is true, the document will be read by given processor (filtering is done on MongoDB
     * side as well).
     * <p>
     * Additional, temporary column {@code _thisPartition} will be created during this calculation and will be removed
     * at the end of calculation.
     */
    @Nonnull
    static List<Bson> partitionAggregate(int totalParallelism, int processorIndex, boolean stream) {
        List<Bson> aggregateList = new ArrayList<>(3);

        String hashFunction = "function hash(s) { \n" +
                "  var hash = 0, i, chr;\n" +
                "  if (s.length === 0) return hash;\n" +
                "  for (i = 0; i < s.length; i++) {\n" +
                "    chr = s.charCodeAt(i);\n" +
                "    hash = ((hash << 5) - hash) + chr;\n" +
                "    hash |= 0;\n" +
                "  }\n" +
                "  return hash;\n" +
                "}\n";

        String moduloPart = " %" + totalParallelism + " == " + processorIndex + ";\n";
        String code = "function(id) {\n"
                + "if (id instanceof ObjectId)"
                + "    return id === null ? -1 : id.getTimestamp().getTime() " + moduloPart
                + "if (typeof(id) === 'number' || typeof(id) === 'bigint') return id " + moduloPart
                + "if (typeof(id) === 'string') {\n"
                + hashFunction
                + "    return hash(id) " + moduloPart
                + "}\n"
                + "else return 0;\n"
                + "}";

        String idRef = stream ? "$fullDocument._id" : "$_id";
        Document functionInv = new Document("$function",
                new Document("lang", "js")
                        .append("body", code)
                        .append("args", new BsonArray(singletonList(new BsonString(idRef)))));

        aggregateList.add(addFields(new Field<>("_thisPartition", functionInv)));
        aggregateList.add(match(Filters.eq("_thisPartition", true)));
        aggregateList.add(unset("_thisPartition"));
        return aggregateList;
    }

    /**
     * Reads data from the cursor up to {@code maxSize} elements or until
     * first null from {@link MongoCursor#tryNext()}
     */
    static <T> List<T> readChunk(MongoCursor<T> cursor, int maxSize) {
        List<T> chunk = new ArrayList<>(maxSize);
        int count = 0;
        boolean eagerEnd = false;
        while (count < maxSize && !eagerEnd) {
            // note: do not use `hasNext` and `next` - those methods blocks for new elements
            // and we don't want to block
            T doc = cursor.tryNext();
            if (doc != null) {
                chunk.add(doc);
                count++;
            } else {
                eagerEnd = true;
            }
        }
        return chunk;
    }

}
