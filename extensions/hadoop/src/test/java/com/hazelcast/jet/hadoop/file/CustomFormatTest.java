/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.hadoop.file;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.hadoop.impl.JobConfigurer;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.file.impl.ReadFileFnProvider;
import javassist.bytecode.ClassFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.hazelcast.jet.hadoop.HadoopSources.COPY_ON_READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CustomFormatTest extends BaseFileFormatTest {

    @Test
    public void unknownFormat() {
        assertThatThrownBy(() -> {
            FileSourceBuilder<Integer> builder = FileSources.files(currentDir + "/target/classes/com/hazelcast/jet/hadoop")
                                                            .glob("*class")
                                                            .format(new UnknownIntegerFormat());
            if (useHadoop) {
                builder.useHadoopForLocalFiles(true);
            }
            BatchSource<Integer> source = builder.build();
            Pipeline p = Pipeline.create();
            p.readFrom(source)
             .writeTo(Sinks.logger());

            HazelcastInstance hz = createHazelcastInstance();
            hz.getJet().newJob(p).join();
        }).hasMessageContaining("FileFormat: unknown-integer-format");
    }

    public static class UnknownIntegerFormat implements FileFormat<Integer> {

        @Nonnull
        @Override
        public String format() {
            return "unknown-integer-format";
        }
    }

    @Test
    public void customFormatTest() {
        FileSourceBuilder<ClassFile> builder = FileSources.files(currentDir + "/target/classes/com/hazelcast/jet/hadoop")
                                                          .glob("*class")
                                                          .format(new ClassFileFormat());


        assertItemsInSource(builder, classFiles ->
                assertThat(classFiles)
                        .extracting(ClassFile::getName)
                        .contains(
                                "com.hazelcast.jet.hadoop.HadoopProcessors",
                                "com.hazelcast.jet.hadoop.HadoopSinks",
                                "com.hazelcast.jet.hadoop.HadoopSources"
                        )
        );

    }

    public static class ClassFileFormat implements FileFormat<ClassFile> {

        @Nonnull
        @Override
        public String format() {
            return "class";
        }
    }

    public static class ClassFileReadFileFnProvider implements ReadFileFnProvider {

        @Nonnull
        @Override
        @SuppressWarnings("unchecked")
        public <T> FunctionEx<Path, Stream<T>> createReadFileFn(@Nonnull FileFormat<T> format) {
            return path -> {
                try (FileInputStream fin = new FileInputStream(path.toFile())) {
                    ClassFile classFile = new ClassFile(new DataInputStream(fin));
                    return (Stream<T>) Stream.of(classFile);
                } catch (IOException e) {
                    throw new JetException(e);
                }
            };
        }

        @Nonnull
        @Override
        public String format() {
            return "class";
        }
    }

    public static class ClassFileJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.getConfiguration().setBoolean(COPY_ON_READ, false);
            job.setInputFormatClass(ClassFileInputFormat.class);
        }

        @Override
        public BiFunctionEx<?, ?, ?> projectionFn() {
            return (k, v) -> v;
        }

        @Nonnull
        @Override
        public String format() {
            return "class";
        }
    }

    public static class ClassFileInputFormat extends FileInputFormat<NullWritable, Object> {

        @Override
        public RecordReader<NullWritable, Object> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new RecordReader<NullWritable, Object>() {

                private transient ClassFile current;
                private FileSplit fileSplit;
                private Configuration conf;
                private boolean processed;

                @Override
                public void initialize(InputSplit split, TaskAttemptContext context) {
                    this.fileSplit = (FileSplit) split;
                    this.conf = context.getConfiguration();
                }

                @Override
                public boolean nextKeyValue() throws IOException {
                    if (!processed) {
                        org.apache.hadoop.fs.Path file = fileSplit.getPath();
                        FileSystem fs = file.getFileSystem(conf);
                        FSDataInputStream in = null;
                        try {
                            in = fs.open(file);
                            current = new ClassFile(in);
                        } finally {
                            IOUtils.closeStream(in);
                        }
                        processed = true;
                        return true;
                    }
                    return false;
                }


                @Override
                public NullWritable getCurrentKey() {
                    return NullWritable.get();
                }

                @Override
                public ClassFile getCurrentValue() {
                    return current;
                }

                @Override
                public float getProgress() {
                    return processed ? 1.0f : 0.0f;
                }

                @Override
                public void close() {
                }
            };

        }

        @Override
        protected boolean isSplitable(JobContext context, org.apache.hadoop.fs.Path file) {
            return false;
        }
    }
}
