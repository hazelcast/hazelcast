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

package com.hazelcast.jet.examples.hadoop.parquet;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.examples.hadoop.generated.User;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.stream.IntStream;

/**
 * A sample which reads records from Apache Parquet file from Hadoop
 * using Apache Avro schema, filters and writes back to Hadoop
 * using Apache Parquet output format with the same schema.
 */
public class HadoopParquet {

    private static final String MODULE_DIRECTORY = moduleDirectory();
    private static final String INPUT_PATH = MODULE_DIRECTORY + "/hdfs-parquet-input";
    private static final String OUTPUT_PATH = MODULE_DIRECTORY + "/hdfs-parquet-output";

    private static final String INPUT_BUCKET_NAME = "jet-hdfs-parquet-input";
    private static final String OUTPUT_BUCKET_NAME = "jet-hdfs-parquet-output";
    private static final String ACCESS_KEY = "";
    private static final String SECRET_KEY = "";

    private static Pipeline buildPipeline(Configuration configuration) {
        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.<String, User, User>inputFormat(configuration, (s, user) -> user))
         .filter(user -> user.get(3).equals(Boolean.TRUE))
         .peek()
         .writeTo(HadoopSinks.outputFormat(configuration, o -> null, o -> o));
        return p;
    }

    /**
     * To run the example on Amazon S3 upload the created file `file.parquet`
     * to the `INPUT_BUCKET_NAME` and fill `ACCESS_KEY`, `SECRET_KEY` fields.
     */
    public static void main(String[] args) throws Exception {
        new HadoopParquet().go();

//        new HadoopParquet().s3();
    }

    private void go() throws Exception {
        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);
        FileSystem.get(new Configuration()).delete(outputPath, true);
        try {
            createParquetFile();
            JetInstance jet = Jet.bootstrappedInstance();

            Configuration jobConfig = createJobConfig(Job.getInstance(), inputPath, outputPath);
            jet.newJob(buildPipeline(jobConfig)).join();

        } finally {
            Jet.shutdownAll();
        }
    }

    private void s3() throws Exception {
        Path inputPath = new Path("s3a://" + INPUT_BUCKET_NAME + "/");
        Path outputPath = new Path("s3a://" + OUTPUT_BUCKET_NAME + "/");

        try {
            JetInstance jet = Jet.bootstrappedInstance();

            Configuration jobConfig = createJobConfig(jobWithS3AccessKeys(), inputPath, outputPath);
            jet.newJob(buildPipeline(jobConfig)).join();

        } finally {
            Jet.shutdownAll();
        }
    }

    private Job jobWithS3AccessKeys() throws IOException {
        Job job = Job.getInstance();
        Configuration configuration = job.getConfiguration();
        configuration.set("fs.s3a.access.key", ACCESS_KEY);
        configuration.set("fs.s3a.secret.key", SECRET_KEY);
        return job;
    }

    private Configuration createJobConfig(Job job, Path inputPath, Path outputPath) throws IOException {
        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, User.SCHEMA$);
        AvroParquetInputFormat.addInputPath(job, inputPath);
        AvroParquetInputFormat.setAvroReadSchema(job, User.SCHEMA$);
        return job.getConfiguration();
    }

    private void createParquetFile() throws IOException {
        Path inputPath = new Path(INPUT_PATH);
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(inputPath, true);
        Path filePath = new Path(inputPath, "file.parquet");

        ParquetWriter<User> writer = AvroParquetWriter.
                <User>builder(filePath)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(User.SCHEMA$)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();

        IntStream.range(0, 100)
                 .mapToObj(i -> new User("name" + i, "pass" + i, i, i % 2 == 0))
                 .forEach(user -> Util.uncheckRun(() -> writer.write(user)));
        writer.close();
        fs.close();
    }

    private static String moduleDirectory() {
        String resourcePath = HadoopParquet.class.getClassLoader().getResource("").getPath();
        return Paths.get(resourcePath).getParent().getParent().toString();
    }

}
