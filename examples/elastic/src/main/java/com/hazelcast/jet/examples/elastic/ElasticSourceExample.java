/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.elastic;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.elastic.ElasticSources;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.elasticsearch.action.search.SearchRequest;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.elastic.ElasticClients.client;
import static com.hazelcast.jet.pipeline.Pipeline.create;

/**
 * Example for the Elastic Source
 *
 * The pipeline in the example:
 * - reads all documents from Elastic running on localhost:9200
 * - maps role and name fields
 * - aggregates all names by role name
 * - writes aggregated result to an observable
 *
 * The results are then printed to the standard output of the submitting client.
 */
public class ElasticSourceExample {

    private static final String ROLES_OBSERVABLE = "roles";

    public static void main(String[] args) {
        try {
            Pipeline p = create();
            BatchSource<Tuple2<String, String>> elasticsearch = ElasticSources.elastic(
                    () -> client("localhost", 9200),
                    SearchRequest::new,
                    hit -> {
                        Map<String, Object> map = hit.getSourceAsMap();
                        return tuple2((String) map.get("role"), (String) map.get("name"));
                    }
            );

            p.readFrom(elasticsearch)
             .groupingKey(Tuple2::getKey)
             .aggregate(mapping(Tuple2::getValue, toList()))
             .writeTo(Sinks.observable(ROLES_OBSERVABLE));

            JetInstance jet = Jet.bootstrappedInstance();

            Observable<Entry<String, List<String>>> roles = jet.getObservable(ROLES_OBSERVABLE);
            roles.addObserver(System.out::println);

            jet.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

}
