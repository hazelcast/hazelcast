/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.hadoop.HdfsSinks;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.IntStream;

/**
 * A sample which reads records from HDFS using Apache Avro input format,
 * filters and writes back to HDFS using Apache Avro output format
 */
public class HadoopAvro {

    private static final String MODULE_DIRECTORY = moduleDirectory();
    private static final String INPUT_PATH = MODULE_DIRECTORY + "/hdfs-avro-input";
    private static final String OUTPUT_PATH = MODULE_DIRECTORY + "/hdfs-avro-output";

    private static Pipeline buildPipeline(JobConf jobConfig) {
        Pipeline p = Pipeline.create();
        p.readFrom(HdfsSources.<AvroWrapper<User>, NullWritable>hdfs(jobConfig))
         .filter(entry -> entry.getKey().datum().get(3).equals(Boolean.TRUE))
         .peek(entry -> entry.getKey().datum().toString())
         .writeTo(HdfsSinks.hdfs(jobConfig));
        return p;
    }

    public static void main(String[] args) throws Exception {
        new HadoopAvro().go();
    }

    private void go() throws Exception {
        try {
            createAvroFile();
            JetInstance jet = Jet.newJetInstance();

            JobConf jobConfig = createJobConfig();
            jet.newJob(buildPipeline(jobConfig)).join();

        } finally {
            Jet.shutdownAll();
        }
    }

    private JobConf createJobConfig() throws IOException {
        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);

        FileSystem.get(new Configuration()).delete(outputPath, true);

        JobConf jobConfig = new JobConf();
        jobConfig.setInputFormat(AvroInputFormat.class);
        jobConfig.setOutputFormat(AvroOutputFormat.class);
        AvroOutputFormat.setOutputPath(jobConfig, outputPath);
        AvroInputFormat.addInputPath(jobConfig, inputPath);
        jobConfig.set(AvroJob.OUTPUT_SCHEMA, User.SCHEMA.toString());
        jobConfig.set(AvroJob.INPUT_SCHEMA, User.SCHEMA.toString());
        return jobConfig;
    }

    private void createAvroFile() throws IOException {
        Path inputPath = new Path(INPUT_PATH);
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(inputPath, true);

        DataFileWriter<User> fileWriter = new DataFileWriter<>(new GenericDatumWriter<User>(User.SCHEMA));

        fileWriter.create(User.SCHEMA, fs.create(new Path(inputPath, "file.avro")));
        IntStream.range(0, 100)
                 .mapToObj(i -> new User("name" + i, "pass" + i, i, i % 2 == 0))
                 .forEach(user -> Util.uncheckRun(() -> fileWriter.append(user)));
        fileWriter.close();
        fs.close();
    }

    private static String moduleDirectory() {
        String resourcePath = HadoopAvro.class.getClassLoader().getResource("").getPath();
        return Paths.get(resourcePath).getParent().getParent().toString();
    }

}
