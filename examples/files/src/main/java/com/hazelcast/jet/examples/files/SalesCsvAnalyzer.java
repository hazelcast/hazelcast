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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.Serializable;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Demonstrates the usage of the file {@link Sources#filesBuilder sources}
 * in a job that reads a sales records in a CSV, filters possible contactless
 * transactions, aggregates transaction counts per payment type and prints
 * the results to standard output.
 * <p>
 * The sample CSV file is in {@code {module.dir}/data/sales.csv}.
 */
public class SalesCsvAnalyzer {

    private static Pipeline buildPipeline(String sourceDir) {
        Pipeline p = Pipeline.create();

        BatchSource<SalesRecordLine> source = Sources.filesBuilder(sourceDir)
             .glob("*.csv")
             .build(path -> Files.lines(path).skip(1).map(SalesRecordLine::parse));

        p.readFrom(source)
         .filter(record -> record.getPrice() < 30)
         .groupingKey(SalesRecordLine::getPaymentType)
         .aggregate(AggregateOperations.counting())
         .writeTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage:");
            System.err.println("  " + SalesCsvAnalyzer.class.getSimpleName() + " <sourceDir>");
            System.exit(1);
        }
        final String sourceDir = args[0];

        Pipeline p = buildPipeline(sourceDir);

        JetInstance instance = Jet.bootstrappedInstance();
        try {
            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Immutable data transfer object mapping the sales transaction.
     */
    private static class SalesRecordLine implements Serializable {
        private static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US);

        private long time;
        private String product;
        private double price;
        private String paymentType;
        private String name;
        private String city;
        private String state;
        private String country;

        public static SalesRecordLine parse(String line) {
            String[] split = line.split(",");
            SalesRecordLine record = new SalesRecordLine();
            record.time = LocalDateTime.parse(split[0], DATE_TIME_FORMATTER).toInstant(ZoneOffset.UTC).toEpochMilli();
            record.product = split[1];
            record.price = Double.parseDouble(split[2]);
            record.paymentType = split[3];
            record.name = split[4];
            record.city = split[5];
            record.state = split[6];
            record.country = split[7];
            return record;
        }

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

        @Override
        public String toString() {
            return "SalesRecordLine{" +
                    "time=" + time +
                    ", product='" + product + '\'' +
                    ", price=" + price +
                    ", paymentType='" + paymentType + '\'' +
                    ", name='" + name + '\'' +
                    ", city='" + city + '\'' +
                    ", state='" + state + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }
    }
}
