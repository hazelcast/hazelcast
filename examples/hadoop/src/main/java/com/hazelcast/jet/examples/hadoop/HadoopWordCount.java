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

package com.hazelcast.jet.examples.hadoop;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.regex.Pattern;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Word count example adapted to read from and write to Hadoop instead of Jet
 * in-memory maps.
 * <p>
 * For more details about the word count pipeline itself, please see the JavaDoc
 * for the {@code WordCount} class in {@code wordcount} sample.
 * <p>
 * {@link HadoopSources#inputFormat(Configuration, BiFunctionEx)} is a source
 * that can be used for reading from HDFS given a {@code JobConf}
 * with input paths and input formats. The files in the input folder
 * will be split among Jet processors, using {@code InputSplit}s.
 * <p>
 * {@link HadoopSinks#outputFormat(Configuration, FunctionEx, FunctionEx)}
 * writes the output to the given output path, with each
 * processor writing to a single file within the path. The files are
 * identified by the member ID and the local ID of the writing processor.
 * Unlike in MapReduce, the data in the output files is not sorted by key.
 * <p>
 * In this example, files are read from and written to using {@code
 * TextInputFormat} and {@code TextOutputFormat} respectively, but the
 * example can be adjusted to be used with any input/output format.
 */
public class HadoopWordCount {

    private static final String OUTPUT_PATH = "hadoop-word-count";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(HadoopWordCount.class.getResource("/books").getPath());
        Path outputPath = new Path(OUTPUT_PATH);
        // delete the output directory, if already exists
        FileSystem.get(new Configuration()).delete(outputPath, true);
        executeSample(new JobConf(), inputPath, outputPath);
    }

    public static void executeSample(JobConf jobConf, Path inputPath, Path outputPath) {
        try {
            JetInstance jet = Jet.bootstrappedInstance();
            System.out.print("\nCounting words from " + inputPath);
            long start = nanoTime();
            Pipeline p = buildPipeline(jobConf, inputPath, outputPath);
            jet.newJob(p).join();
            System.out.println("Done in " + NANOSECONDS.toMillis(nanoTime() - start) + " milliseconds.");
            System.out.println("Output written to " + outputPath);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline(JobConf jobConf, Path inputPath, Path outputPath) {
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(jobConf, outputPath);
        TextInputFormat.addInputPath(jobConf, inputPath);

        final Pattern regex = Pattern.compile("\\W+");
        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.inputFormat(jobConf, (k, v) -> v.toString()))
         .flatMap(line -> traverseArray(regex.split(line.toLowerCase())).filter(w -> !w.isEmpty()))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .writeTo(HadoopSinks.outputFormat(jobConf));
        return p;
    }

}
