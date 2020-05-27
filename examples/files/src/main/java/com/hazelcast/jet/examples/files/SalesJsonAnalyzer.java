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

package com.hazelcast.jet.examples.files;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.Serializable;

/**
 * Demonstrates the usage of the file {@link Sources#filesBuilder
 * sources} in a job that reads a sales records in a JSON file, filters
 * higher priced records, aggregates record counts per payment type and
 * prints the results to standard output.
 * <p>
 * The sample JSON file is in {@code {module.dir}/data/sales.json}.
 */
public class SalesJsonAnalyzer {

    private static Pipeline buildPipeline(String sourceDir) {
        Pipeline p = Pipeline.create();

        BatchSource<SalesRecord> source = Sources.json(sourceDir, SalesRecord.class);
        p.readFrom(source)
         .filter(record -> record.price < 30)
         .groupingKey(r -> r.paymentType)
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage:");
            System.err.println("  " + SalesJsonAnalyzer.class.getSimpleName() + " <sourceDir>");
            System.exit(1);
        }
        String sourceDir = args[0];

        Pipeline p = buildPipeline(sourceDir);

        JetInstance instance = Jet.bootstrappedInstance();
        try {
            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Data transfer object mapping the sales transaction. See that
     * {@code paymentType} field is mapped to `payment_type` using
     * <a href="https://github.com/FasterXML/jackson-annotations/wiki/Jackson-Annotations">
     * Jackson Annotations</a>
     */
    private static class SalesRecord implements Serializable {

        @JsonProperty(value = "payment_type")
        public String paymentType;

        public long time;
        public String product;
        public double price;
        public String name;
        public String address;
        public String city;
        public String state;
        public String country;

        @Override
        public String toString() {
            return "SalesRecord{" +
                    "time=" + time +
                    ", product='" + product + '\'' +
                    ", price=" + price +
                    ", paymentType='" + paymentType + '\'' +
                    ", name='" + name + '\'' +
                    ", address='" + address + '\'' +
                    ", city='" + city + '\'' +
                    ", state='" + state + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }
    }
}
