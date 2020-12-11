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

package com.hazelcast.jet.examples.files.unifiedapi;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.examples.files.unifiedapi.generated.AvroTrade;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;

import java.nio.file.Paths;

/**
 * Demonstrates usage of {@link FileSources#files(String)} and various file formats from {@link FileFormat}
 * <p>
 * Usage:
 * com.hazelcast.jet.examples.files.unifiedapi.TradeAnalyzerJob <csv|json|avro|parquet> <sourceDir>
 * <p>
 * Reads files of given type, expecting corresponding format, and sums the quantity sold for each trade instrument and
 * prints to result to the log.
 * <p>
 * CSV - the .csv file is expected to contain header with columns matching the fields of the class specified by
 * {@link FileFormat#csv(Class)}
 * <p>
 * JSON - the .jsonl files is expected to contain one valid json document per line, each document must have fields
 * matching the fields of the class specified by {@link FileFormat#json(Class)}
 * <p>
 * AVRO - the Avro file is expected to have a schema compatible with the class specified by
 * {@link FileFormat#avro(Class)}
 * <p>
 * Parquet - the type of the items read from the Parquet source is specified by the schema in the parquet file.
 */
public class TradeAnalyzerJob {

    private static Pipeline buildPipeline(String sourceDir, String type) {
        Pipeline p = Pipeline.create();

        BatchStage<Trade> trades;
        BatchSource<Trade> source;
        switch (type) {
            case "csv":
                source = FileSources.files(sourceDir)
                                    .glob("*." + type)
                                    .format(FileFormat.csv(Trade.class))
                                    .build();
                trades = p.readFrom(source);
                break;

            case "json":
                source = FileSources.files(sourceDir)
                                    .glob("*." + type + "*")
                                    .format(FileFormat.json(Trade.class))
                                    .build();
                trades = p.readFrom(source);
                break;

            case "avro":
                source = FileSources.files(sourceDir)
                                    .glob("*." + type)
                                    .format(FileFormat.avro(Trade.class))
                                    .build();
                trades = p.readFrom(source);
                break;

            case "parquet":
                BatchSource<AvroTrade> parquetSource = FileSources.files(sourceDir)
                                                                  .glob("*." + type)
                                                                  .format(FileFormat.<AvroTrade>parquet())
                                                                  .useHadoopForLocalFiles(true)
                                                                  .build();
                trades = p.readFrom(parquetSource)
                          .map(avroTrade -> new Trade(avroTrade.getTime(), avroTrade.getTicker().toString(),
                                  avroTrade.getQuantity(), avroTrade.getPrice()));
                break;

            default:
                throw new RuntimeException("Unknown type: " + type);
        }


        trades.groupingKey(Trade::getTicker)
              .aggregate(AggregateOperations.summingLong(Trade::getQuantity))
              .sort((o1, o2) -> Long.compare(o1.getValue(), o2.getValue()))
              .writeTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage:");
            System.err.println("  " + TradeAnalyzerJob.class.getSimpleName() + " <csv|json|avro|parquet> <sourceDir>");
            System.exit(1);
        }
        final String type = args[0];
        final String sourceDir = args[1];

        Pipeline p = buildPipeline(Paths.get(sourceDir).toAbsolutePath().toString(), type);

        JetInstance instance = Jet.bootstrappedInstance();
        try {
            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
