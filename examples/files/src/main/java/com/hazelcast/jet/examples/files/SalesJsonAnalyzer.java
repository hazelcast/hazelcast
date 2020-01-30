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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Demonstrates the usage of the file {@link Sources#filesBuilder sources}
 * in a job that reads a sales records in a JSON file, filters possible contactless
 * transactions, aggregates transaction counts per payment type and prints
 * the results to standard output.
 * <p>
 * The sample JSON file is in {@code {module.dir}/data/sales.json}.
 */
public class SalesJsonAnalyzer {

    private static Pipeline buildPipeline(String sourceDir) {
        Pipeline p = Pipeline.create();

        BatchSource<SalesRecord> source = Sources.filesBuilder(sourceDir)
            .glob("*.json")
            .build(path -> readJsonArray(path, SalesRecord.class));
        p.readFrom(source)
         .filter(record -> record.getPrice() < 30)
         .groupingKey(SalesRecord::getPaymentType)
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.logger());

        return p;
    }

    /**
     * Read a file denoted by the given {@code filePath} that contains a single
     * JSON array of objects of type {@code type}. Returns the file contents as
     * a {@code Stream}.
     */
    private static <T> Stream<T> readJsonArray(Path filePath, Type type) throws IOException {
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(Files.newBufferedReader(filePath, UTF_8));
        reader.beginArray();

        // in java9+ you can use this simpler code
//        return Stream.generate((SupplierEx<T>) () -> reader.hasNext() ? gson.fromJson(reader, type) : null)
//                .takeWhile(Objects::nonNull)
//                .onClose((RunnableEx) reader::close);

        Iterator<T> iterator = new Iterator<T>() {
            @Override
            public boolean hasNext() {
                try {
                    return reader.hasNext();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public T next() {
                return gson.fromJson(reader, type);
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL), false)
                .onClose((RunnableEx) reader::close);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage:");
            System.err.println("  " + SalesJsonAnalyzer.class.getSimpleName() + " <sourceDir>");
            System.exit(1);
        }
        final String sourceDir = args[0];

        Pipeline p = buildPipeline(sourceDir);

        JetInstance instance = Jet.newJetInstance();
        try {
            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Immutable data transfer object mapping the sales transaction.
     */
    private static class SalesRecord implements Serializable {
        private long time;
        private String product;
        private double price;
        private String paymentType;
        private String name;
        private String address;
        private String city;
        private String state;
        private String country;

        public long getTime() {
            return time;
        }

        public String getProduct() {
            return product;
        }

        public double getPrice() {
            return price;
        }

        public String getPaymentType() {
            return paymentType;
        }

        public String getName() {
            return name;
        }

        public String getCity() {
            return city;
        }

        public String getState() {
            return state;
        }

        public String getCountry() {
            return country;
        }

        public String getAddress() {
            return address;
        }

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
