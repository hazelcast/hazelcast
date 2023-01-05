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
package com.hazelcast.jet.mongodb;

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

    @Nonnull
    static List<Bson> partitionAggregate(int totalParallelism, int processorIndex, boolean stream) {
        List<Bson> aggregateList = new ArrayList<>(3);

        String code = "function(s) {\n"
                + "    return s === null ? -1 : s.getTimestamp().getTime() %"
                        + totalParallelism + " == " + processorIndex + ";\n"
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
}
