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
 * Word count example adapted to read from and write to Azure Data Lake Storage
 * using HDFS source and sink.
 * <p>
 * The job reads objects from the given container {@link #FOLDER_NAME} and
 * writes the word count results to a folder inside that folder.
 * <p>
 * To be able to read from and write to Azure Data Lake Storage, HDFS needs
 * {@code hadoop-azure-datalake} as dependency and credentials for the storage
 * account.
 *
 * @see <a href="https://hadoop.apache.org/docs/r3.0.3/hadoop-azure-datalake/index.html">
 * Hadoop Azure Data Lake Support</a> for more information
 */
public class AzureDataLake {


    private static final String ACCOUNT_NAME = "";
    private static final String CLIENT_ID = "";
    private static final String TENANT_ID = "";
    private static final String CLIENT_CREDENTIALS = "";
    private static final String FOLDER_NAME = "jet-azure-data-lake-folder";

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("adl://" + ACCOUNT_NAME + ".azuredatalakestore.net/" + FOLDER_NAME);
        Path outputPath = new Path("adl://" + ACCOUNT_NAME + ".azuredatalakestore.net/" + FOLDER_NAME + "/results");

        JobConf jobConf = new JobConf();
        jobConf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential");
        jobConf.set("fs.adl.oauth2.refresh.url", "https://login.microsoftonline.com/" + TENANT_ID + "/oauth2/token");
        jobConf.set("fs.adl.oauth2.client.id", CLIENT_ID);
        jobConf.set("fs.adl.oauth2.credential", CLIENT_CREDENTIALS);

        HadoopWordCount.executeSample(jobConf, inputPath, outputPath);
    }
}
