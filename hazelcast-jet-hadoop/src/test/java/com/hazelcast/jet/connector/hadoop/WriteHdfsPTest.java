/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.connector.hadoop;

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Processors.writeList;
import static com.hazelcast.jet.connector.hadoop.ReadHdfsP.readHdfs;
import static com.hazelcast.jet.connector.hadoop.WriteHdfsP.writeHdfs;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_SERIALIZATIONS_KEY;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class WriteHdfsPTest extends JetTestSupport {

    @Parameterized.Parameter(0)
    public Class<? extends OutputFormat> outputFormatClass;

    @Parameterized.Parameter(1)
    public Class<? extends InputFormat> inputFormatClass;

    @Parameterized.Parameters(name = "Executing: {0} {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{TextOutputFormat.class, TextInputFormat.class}, //
                new Object[]{SequenceFileOutputFormat.class, SequenceFileInputFormat.class} //
        );
    }

    @Test
    public void testWriteFile() throws Exception {
        int messageCount = 20;
        String mapName = randomMapName();
        JetInstance instance = createJetMember();
        createJetMember();

        Map<IntWritable, IntWritable> map = IntStream.range(0, messageCount).boxed().collect(Collectors.toMap(IntWritable::new, IntWritable::new));
        instance.getMap(mapName).putAll(map);

        DAG dag = new DAG();
        Vertex producer = dag.newVertex("producer", readMap(mapName))
                .localParallelism(1);

        Path path = getPath();

        JobConf conf = new JobConf();
        conf.setOutputFormat(outputFormatClass);
        conf.setOutputCommitter(FileOutputCommitter.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(conf, path);


        Vertex consumer = dag.newVertex("consumer", writeHdfs(conf))
                .localParallelism(4);

        dag.edge(between(producer, consumer));

        Future<Void> future = instance.newJob(dag).execute();
        assertCompletesEventually(future);


        dag = new DAG();
        JobConf readJobConf = new JobConf();
        readJobConf.setInputFormat(inputFormatClass);
        FileInputFormat.addInputPath(readJobConf, path);
        producer = dag.newVertex("producer", readHdfs(readJobConf))
                .localParallelism(8);

        consumer = dag.newVertex("consumer", writeList("results"))
                .localParallelism(1);

        dag.edge(between(producer, consumer));
        future = instance.newJob(dag).execute();
        assertCompletesEventually(future);


        IList<Object> results = instance.getList("results");
        assertEquals(messageCount, results.size());
    }

    private Path getPath() throws IOException {
        String dirName = Files.createTempDirectory(getClass().getName()).toString();
        return new Path(dirName);
    }

}
