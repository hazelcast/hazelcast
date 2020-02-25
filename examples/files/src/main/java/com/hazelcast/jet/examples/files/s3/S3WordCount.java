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

package com.hazelcast.jet.examples.files.s3;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.s3.S3Sinks;
import com.hazelcast.jet.s3.S3Sources;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static java.lang.System.nanoTime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Word count example adapted to read from and write to S3 bucket. The example
 * uploads some books to the given input bucket and result of the word count
 * is written to output bucket. Set credentials and region information to run
 * the example.
 * <p>
 * For more details about the word count pipeline itself, please see the JavaDoc
 * for the {@code WordCount} class in {@code wordcount} sample.
 * <p>
 * {@link S3Sources#s3(List, String, Charset, SupplierEx, BiFunctionEx)}
 * is a source that can be used for reading from an S3 bucket with the given
 * credentials. The files in the input bucket will be split among Jet
 * processors.
 * <p>
 * {@link S3Sinks#s3(String, String, Charset, SupplierEx, FunctionEx)}
 * writes the output to the given output bucket, with each
 * processor writing to a file within the bucket. The files are
 * identified by a prefix (if provided) and followed by the global processor index.
 * <p>
 */
public class S3WordCount {

    private static final String INPUT_BUCKET = "jet-s3-example-input-bucket";
    private static final String OUTPUT_BUCKET = "jet-s3-example-output-bucket";
    private static final String PREFIX = "books/";

    private static Pipeline buildPipeline() {
        final Pattern regex = Pattern.compile("\\W+");
        Pipeline p = Pipeline.create();
        p.readFrom(S3Sources.s3(
                Collections.singletonList(INPUT_BUCKET),
                PREFIX,
                UTF_8,
                S3WordCount::createClient,
                (name, line) -> line)
        ).flatMap(line -> traverseArray(regex.split(line.toLowerCase())).filter(w -> !w.isEmpty()))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .writeTo(S3Sinks.s3(OUTPUT_BUCKET, PREFIX, UTF_8, S3WordCount::createClient, Object::toString));
        return p;
    }

    private static S3Client createClient() {
        return S3Client.create();
    }

    public static void main(String[] args) throws IOException {
        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "");
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "");
        try {
            System.out.println("Uploading books to bucket " + INPUT_BUCKET);
            uploadBooks(PREFIX);
            JetInstance jet = Jet.bootstrappedInstance();
            System.out.print("\nCounting words from " + INPUT_BUCKET);
            long start = nanoTime();
            Pipeline p = buildPipeline();
            jet.newJob(p).join();
            System.out.println("Done in " + NANOSECONDS.toMillis(nanoTime() - start) + " milliseconds.");
            System.out.println("Output written to " + OUTPUT_BUCKET);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void uploadBooks(String prefix) throws IOException {
        try (S3Client s3Client = createClient()) {
            Path path = Paths.get(S3WordCount.class.getResource("/books").getPath());
            Files.list(path)
                 .limit(10)
                 .forEach(book -> {
                     System.out.println("Uploading file " + book.getFileName().toString() + "...");
                     s3Client.putObject(req -> req.bucket(INPUT_BUCKET).key(prefix + book.getFileName().toString()),
                             RequestBody.fromFile(book.toFile()));
                 });
        }
    }

}
