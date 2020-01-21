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
 * Word count example adapted to read from and write to Google Cloud Storage
 * using HDFS source and sink.
 * <p>
 * The job reads objects from the given bucket {@link #BUCKET_NAME} and writes
 * the word count results to a folder inside that bucket.
 * <p>
 * To be able to read from and write to Google Cloud Storage, HDFS needs couple
 * of dependencies and the json key file for GCS. Necessary dependencies:
 * <ul>
 *     <li>gcs-connector</li>
 *     <li>guava</li>
 * </ul>
 *
 * @see <a href="https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage">
 *     Google Cloud Storage Connector</a> for more information
 */
public class GoogleCloudStorage {

    private static final String JSON_KEY_FILE = "path-to-the-json-key-file";

    private static final String BUCKET_NAME = "jet-gcs-hdfs-example-bucket";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("gs://" + BUCKET_NAME + "/");
        Path outputPath = new Path("gs://" + BUCKET_NAME + "/results");

        JobConf jobConf = new JobConf();
        jobConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        jobConf.set("fs.gs.auth.service.account.enable", "true");
        jobConf.set("fs.gs.auth.service.account.json.keyfile", JSON_KEY_FILE);

        HadoopWordCount.executeSample(jobConf, inputPath, outputPath);
    }
}
