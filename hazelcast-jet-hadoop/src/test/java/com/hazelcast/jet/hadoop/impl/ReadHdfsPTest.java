/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.core.IList;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.hadoop.HdfsSources;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.hadoop.HdfsProcessors.readHdfsP;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.Integer.parseInt;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(ParallelTest.class)
public class ReadHdfsPTest extends HdfsTestSupport {

    private static final String[] ENTRIES = range(0, 4)
            .mapToObj(i -> "key-" + i + " value-" + i + '\n')
            .toArray(String[]::new);

    @Parameterized.Parameter
    public Class<? extends InputFormat> inputFormatClass;

    @Parameterized.Parameter(1)
    public EMapperType mapperType;
    private JobConf jobConf;
    private JetInstance instance;
    private Set<Path> paths = new HashSet<>();

    @Parameterized.Parameters(name = "inputFormatClass={0}, mapper={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{TextInputFormat.class, EMapperType.DEFAULT},
                new Object[]{TextInputFormat.class, EMapperType.CUSTOM},
                new Object[]{TextInputFormat.class, EMapperType.CUSTOM_WITH_NULLS},
                new Object[]{SequenceFileInputFormat.class, EMapperType.DEFAULT},
                new Object[]{SequenceFileInputFormat.class, EMapperType.CUSTOM},
                new Object[]{SequenceFileInputFormat.class, EMapperType.CUSTOM_WITH_NULLS}
        );
    }

    @Before
    public void setup() throws IOException {
        instance = createJetMember();
        jobConf = new JobConf();
        jobConf.setInputFormat(inputFormatClass);

        writeToFile();
        for (Path path : paths) {
            FileInputFormat.addInputPath(jobConf, path);
        }
    }

    @Test
    public void testReadHdfs() {
        DAG dag = new DAG();

        Vertex source = dag.newVertex("source", readHdfsP(jobConf, mapperType.mapper))
                           .localParallelism(4);
        Vertex sink = dag.newVertex("sink", writeListP("sink"))
                         .localParallelism(1);
        dag.edge(between(source, sink));

        Future<Void> future = instance.newJob(dag).getFuture();
        assertCompletesEventually(future);

        IList list = instance.getList("sink");
        assertEquals(expectedSinkSize(), list.size());
        assertTrue(list.get(0).toString().contains("value"));
    }

    @Test
    public void testJus() {
        IListJet sink = DistributedStream
                .fromSource(instance, HdfsSources.hdfs(jobConf, mapperType.mapper), false)
                .collect(DistributedCollectors.toIList("sink"));

        assertEquals(expectedSinkSize(), sink.size());
    }

    private int expectedSinkSize() {
        return mapperType == EMapperType.CUSTOM_WITH_NULLS ? 8 : 16;
    }

    private void writeToFile() throws IOException {
        Configuration conf = new Configuration();
        LocalFileSystem local = FileSystem.getLocal(conf);

        IntStream.range(0, 4).mapToObj(i -> createPath()).forEach(path -> uncheckRun(() -> {
            paths.add(path);
            if (SequenceFileInputFormat.class.equals(inputFormatClass)) {
                writeToSequenceFile(conf, path);
            } else {
                writeToTextFile(local, path);
            }
        }));
    }

    private static void writeToTextFile(LocalFileSystem local, Path path) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(local.create(path)))) {
            for (String value : ENTRIES) {
                writer.write(value);
                writer.flush();
            }
        }
    }

    private static void writeToSequenceFile(Configuration conf, Path path) throws IOException {
        IntWritable key = new IntWritable();
        Text value = new Text();
        Option fileOption = Writer.file(path);
        Option keyClassOption = Writer.keyClass(key.getClass());
        Option valueClassOption = Writer.valueClass(value.getClass());
        try (Writer writer = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption)) {
            for (int i = 0; i < ENTRIES.length; i++) {
                key.set(i);
                value.set(ENTRIES[i]);
                writer.append(key, value);
            }
        }
    }

    private Path createPath() {
        try {
            String fileName = Files.createTempFile(getClass().getName(), null).toString();
            return new Path(fileName);
        } catch (IOException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    private enum EMapperType {
        DEFAULT(Util::entry),
        CUSTOM((k, v) -> v.toString()),
        CUSTOM_WITH_NULLS((k, v) -> parseInt(v.toString().substring(4, 5)) % 2 == 0 ? v.toString() : null);

        private final DistributedBiFunction<?, Text, ?> mapper;

        EMapperType(DistributedBiFunction<?, Text, ?> mapper) {
            this.mapper = mapper;
        }
    }
}
