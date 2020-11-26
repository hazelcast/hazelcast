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

package com.hazelcast.jet.examples.files.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileSources;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Word count example that reads from S3 and writes output to the log.
 * <p>
 * Usage:
 * com.hazelcast.jet.examples.files.cloud.GCSWordCount <inputPath>
 * <p>
 * The input path has the following format:
 * gs://<bucket name>/<path>
 * <p>
 * The following dependency is required to run this example:
 * <pre>{@code <dependency>
 *         <groupId>com.hazelcast.jet</groupId>
 *         <artifactId>hazelcast-jet-files-gcs</artifactId>
 *         <version>${hazelcast-jet.version}</version>
 * </dependency>}</pre>
 */
public class GCSWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage:");
            System.err.println("  " + GCSWordCount.class.getSimpleName() + " <inputPath>");
            System.exit(1);
        }
        String inputPath = args[0];

        try {
            JetInstance jet = Jet.bootstrappedInstance();
            System.out.print("\nCounting words from " + inputPath);
            long start = nanoTime();
            Pipeline p = new GCSWordCount().buildPipeline(inputPath);
            jet.newJob(p).join();
            System.out.println("Done in " + NANOSECONDS.toMillis(nanoTime() - start) + " milliseconds.");
        } finally {
            Jet.shutdownAll();
        }
    }

    private Pipeline buildPipeline(String inputPath) throws IOException {
        final Pattern regex = Pattern.compile("\\W+");


        JsonNode keyJson = new ObjectMapper().readTree(new File(System.getProperty("google.key.file")));
        BatchSource<String> source = FileSources
                .files(inputPath)
                .option("fs.gs.auth.service.account.private.key.id", keyJson.get("private_key_id").textValue())
                .option("fs.gs.auth.service.account.private.key", keyJson.get("private_key").textValue())
                .option("fs.gs.auth.service.account.email", keyJson.get("client_email").textValue())
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .flatMap(line -> traverseArray(regex.split(line.toLowerCase())).filter(w -> !w.isEmpty()))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .aggregate(topN(10, ComparatorEx.comparing(Entry::getValue)))
         .writeTo(Sinks.logger());
        return p;
    }

}
