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

package com.hazelcast.jet.examples.hadoop.cloud;

import com.hazelcast.jet.examples.hadoop.HadoopWordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * Word count example adapted to read from and write to Amazon S3 object storage
 * using HDFS source and sink.
 * <p>
 * The job reads objects from the given bucket {@link #BUCKET_NAME} and writes
 * the word count results to a folder inside that bucket.
 * <p>
 * To be able to read from and write to Amazon S3 object storage, HDFS needs
 * couple of dependencies and the credentials for S3. Necessary dependencies:
 * <ul>
 *     <li>hadoop-aws</li>
 *     <li>hadoop-client</li>
 * </ul>
 *
 * @see <a href="https://hadoop.apache.org/docs/r3.0.3/hadoop-aws/tools/hadoop-aws/index.html">
 *     Hadoop-AWS Module</a> for more information
 */
public class AmazonS3 {

    private static final String ACCESS_KEY = "";
    private static final String SECRET_KEY = "";

    private static final String BUCKET_NAME = "jet-s3-hdfs-example-bucket";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("s3a://" + BUCKET_NAME + "/");
        Path outputPath = new Path("s3a://" + BUCKET_NAME + "/results/");

        JobConf jobConf = new JobConf();
        jobConf.set("fs.s3a.access.key", ACCESS_KEY);
        jobConf.set("fs.s3a.secret.key", SECRET_KEY);

        HadoopWordCount.executeSample(jobConf, inputPath, outputPath);
    }
}
