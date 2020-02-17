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

package com.hazelcast.jet.examples.hadoop.avro;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.examples.hadoop.generated.User;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.IntStream;

/**
 * A sample which reads records from Hadoop using Apache Avro input format,
 * filters and writes back to Hadoop using Apache Avro output format
 */
public class HadoopAvro {

    private static final String MODULE_DIRECTORY = moduleDirectory();
    private static final String INPUT_PATH = MODULE_DIRECTORY + "/hadoop-avro-input";
    private static final String OUTPUT_PATH = MODULE_DIRECTORY + "/hadoop-avro-output";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);

        FileSystem.get(new Configuration()).delete(outputPath, true);

        createAvroFile();

        executeSample(createJobConfig(inputPath, outputPath));
    }

    public static void executeSample(Configuration configuration) {
        try {
            JetInstance jet = Jet.bootstrappedInstance();
            jet.newJob(buildPipeline(configuration)).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void createAvroFile() throws IOException {
        Path inputPath = new Path(INPUT_PATH);
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(inputPath, true);

        DataFileWriter<User> fileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(User.class));
        fileWriter.create(User.SCHEMA$, fs.create(new Path(inputPath, "file.avro")));
        IntStream.range(0, 100)
                 .mapToObj(i -> new User("name" + i, "pass" + i, i, i % 2 == 0))
                 .forEach(user -> Util.uncheckRun(() -> fileWriter.append(user)));
        fileWriter.close();
        fs.close();
    }

    private static Pipeline buildPipeline(Configuration configuration) {
        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.<AvroKey<User>, NullWritable, User>inputFormat(configuration, (key, val) -> key.datum()))
         .filter(user -> user.get(3).equals(Boolean.TRUE))
         .peek()
         .writeTo(HadoopSinks.outputFormat(configuration, AvroKey::new, user -> null));
        return p;
    }

    private static String moduleDirectory() {
        String resourcePath = HadoopAvro.class.getClassLoader().getResource("").getPath();
        return Paths.get(resourcePath).getParent().getParent().toString();
    }

    public static Configuration createJobConfig(Path inputPath, Path outputPath) throws IOException {
        FileSystem.get(new Configuration()).delete(outputPath, true);

        Job job = Job.getInstance();
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroKeyInputFormat.addInputPath(job, inputPath);
        AvroKeyOutputFormat.setOutputPath(job, outputPath);
        AvroJob.setInputKeySchema(job, User.SCHEMA$);
        AvroJob.setOutputKeySchema(job, User.SCHEMA$);
        return job.getConfiguration();
    }

}
