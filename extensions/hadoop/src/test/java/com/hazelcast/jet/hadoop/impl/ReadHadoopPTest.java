/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.collection.IList;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.hadoop.impl.ReadHadoopPTest.EMapperType.CUSTOM_WITH_NULLS;
import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class ReadHadoopPTest extends HadoopTestSupport {

    private static final String[] VALUES = {
            "value-0\n",
            "value-1\n",
            "value-2\n",
            "value-3\n"
    };

    @Parameter(0)
    public Class inputFormatClass;

    @Parameter(1)
    public EMapperType projectionType;

    @Parameter(2)
    public boolean sharedFileSystem;

    private Configuration jobConf;
    private Path directory;
    private final Set<org.apache.hadoop.fs.Path> paths = new HashSet<>();

    @Parameters(name = "inputFormat={0}, mapper={1}, sharedFileSystem={2}")
    public static Collection<Object[]> parameters() {
        return cartesianProduct(
                List.of(
                        org.apache.hadoop.mapred.TextInputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
                        org.apache.hadoop.mapred.SequenceFileInputFormat.class,
                        org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class),
                List.of(EMapperType.values()),
                List.of(true, false));
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void setup() throws IOException {
        directory = Files.createTempDirectory(getClass().getSimpleName());
        createInputFiles();
        createConfiguration();
    }

    @After
    public void after() {
        if (directory != null) {
            IOUtil.delete(directory.toFile());
            directory = null;
        }
    }

    private void createConfiguration() throws IOException {
        if (inputFormatClass.getPackage().getName().contains("mapreduce")) {
            Job job = Job.getInstance();
            job.setInputFormatClass(inputFormatClass);
            for (org.apache.hadoop.fs.Path path : paths) {
                org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, path);
            }
            jobConf = job.getConfiguration();
        } else {
            JobConf jobConf = new JobConf();
            this.jobConf = jobConf;
            jobConf.setInputFormat(inputFormatClass);
            for (org.apache.hadoop.fs.Path path : paths) {
                org.apache.hadoop.mapred.FileInputFormat.addInputPath(jobConf, path);
            }
        }
        jobConf.setBoolean(HadoopSources.SHARED_LOCAL_FS, sharedFileSystem);
    }

    @Test
    public void testReadHdfs() {
        IList<Object> sinkList = instance().getList(randomName());
        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.inputFormat(jobConf, projectionType.mapper))
                .setLocalParallelism(4)
                .writeTo(Sinks.list(sinkList))
                .setLocalParallelism(1);

        instance().getJet().newJob(p).join();
        int expected = paths.size() * VALUES.length * (sharedFileSystem ? 1 : 2);
        assertEquals(projectionType == CUSTOM_WITH_NULLS ? expected / 2 : expected, sinkList.size());
        assertTrue(sinkList.get(0).toString().contains("value"));
    }

    private void createInputFiles() throws IOException {
        Configuration conf = new Configuration();
        LocalFileSystem local = FileSystem.getLocal(conf);

        for (int i = 0; i < 4; i++) {
            org.apache.hadoop.fs.Path path = createPath();
            paths.add(path);
            if (inputFormatClass.getSimpleName().equals("SequenceFileInputFormat")) {
                createInputSequenceFiles(conf, path);
            } else {
                createInputTextFiles(local, path);
            }
        }
    }

    private org.apache.hadoop.fs.Path createPath() {
        try {
            String fileName = Files.createTempFile(directory, getClass().getName(), null).toString();
            return new org.apache.hadoop.fs.Path(fileName);
        } catch (IOException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    private static void createInputTextFiles(LocalFileSystem local, org.apache.hadoop.fs.Path path) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(local.create(path)))) {
            for (String value : VALUES) {
                writer.write(value);
                writer.flush();
            }
        }
    }

    private static void createInputSequenceFiles(Configuration conf, org.apache.hadoop.fs.Path path) throws IOException {
        IntWritable key = new IntWritable();
        Text value = new Text();
        Option fileOption = Writer.file(path);
        Option keyClassOption = Writer.keyClass(key.getClass());
        Option valueClassOption = Writer.valueClass(value.getClass());
        try (Writer writer = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption)) {
            for (int i = 0; i < VALUES.length; i++) {
                key.set(i);
                value.set(VALUES[i]);
                writer.append(key, value);
            }
        }
    }

    public enum EMapperType {
        DEFAULT(Util::entry),
        CUSTOM((k, v) -> v.toString()),
        CUSTOM_WITH_NULLS((k, v) -> parseInt(v.toString().substring(6, 7)) % 2 == 0 ? v.toString() : null);

        private final BiFunctionEx<?, Text, ?> mapper;

        EMapperType(BiFunctionEx<?, Text, ?> mapper) {
            this.mapper = mapper;
        }
    }
}
