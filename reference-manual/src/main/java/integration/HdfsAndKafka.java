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

import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;

public class HdfsAndKafka {

    static void s1() {
        //tag::s1[]
        JobConf jobConfig = new JobConf();
        jobConfig.setInputFormat(TextInputFormat.class);
        jobConfig.setOutputFormat(TextOutputFormat.class);
        TextInputFormat.addInputPath(jobConfig, new Path("input-path"));
        TextOutputFormat.setOutputPath(jobConfig, new Path("output-path"));
        //end::s1[]
    }

    static void s2() throws IOException {
        //tag::s2[]
        Job job = Job.getInstance();
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.addInputPath(job, new Path("input-path"));
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setOutputPath(job, new Path("output-path"));
        Configuration configuration = job.getConfiguration();
        //end::s2[]
    }

    static void s3() throws IOException {
        //tag::s3[]
        Job job = Job.getInstance();
        Configuration configuration = job.getConfiguration();
        configuration.set(HadoopSources.COPY_ON_READ, "false");
        //end::s3[]
    }

    static void s4() {
        JobConf jobConfig = new JobConf();
        //tag::s4[]
        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.inputFormat(jobConfig, (k, v) -> v.toString()))
         .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+"))
                               .filter(w -> !w.isEmpty()))
         .groupingKey(wholeItem())
         .aggregate(counting())
         .writeTo(HadoopSinks.outputFormat(jobConfig));
        //end::s4[]
    }

    static void s5() {
        //tag::s5[]
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
        props.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("value.serializer", IntegerSerializer.class.getCanonicalName());
        props.setProperty("value.deserializer", IntegerDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");

        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(props, "t1", "t2"))
         .withoutTimestamps()
         .writeTo(KafkaSinks.kafka(props, "t3"));
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        //end::s7[]
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        //end::s9[]
    }

    static void s10() {
        //tag::s10[]
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        //end::s11[]
    }

}
