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

package integration;

import com.hazelcast.jet.Util;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.s3.S3Sinks;
import com.hazelcast.jet.s3.S3Sources;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import static java.util.Collections.singletonList;

public class S3 {

    static void s1() {
        //tag::s1[]
        String accessKeyId = "";
        String accessKeySecret = "";
        String prefix = "";

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, accessKeySecret);
        S3Client s3 = S3Client
                .builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(S3Sources.s3(singletonList("input-bucket"), prefix, () -> s3))
         .writeTo(Sinks.logger());
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        String accessKeyId = "";
        String accessKeySecret = "";

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, accessKeySecret);
        S3Client s3 = S3Client
                .builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list("input-list"))
         .writeTo(S3Sinks.s3("output-bucket", () -> s3));
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        JobConf jobConfig = new JobConf();
        jobConfig.setInputFormat(TextInputFormat.class);
        jobConfig.setOutputFormat(TextOutputFormat.class);
        TextInputFormat.addInputPath(jobConfig, new Path("s3a://input-bucket"));
        TextOutputFormat.setOutputPath(jobConfig, new Path("s3a://output-bucket"));


        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.<String, String>inputFormat(jobConfig))
         .map(e -> Util.entry(e.getKey(), e.getValue().toUpperCase()))
         .writeTo(HadoopSinks.outputFormat(jobConfig));
        //end::s3[]
    }
}
