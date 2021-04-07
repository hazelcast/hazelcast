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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.json.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.function.Function;

import static java.util.Collections.emptyIterator;

public class JsonInputFormat extends FileInputFormat<NullWritable, Object> {

    public static final String JSON_INPUT_FORMAT_BEAN_CLASS = "json.bean.class";
    public static final String JSON_MULTILINE = "json.multiline";

    @Override
    public RecordReader<NullWritable, Object> createRecordReader(InputSplit split, TaskAttemptContext context) {

        Configuration configuration = context.getConfiguration();
        String className = configuration.get(JSON_INPUT_FORMAT_BEAN_CLASS);
        Class<?> clazz = className == null ? null : ReflectionUtils.loadClass(className);

        if (acceptMultilineJson(context.getConfiguration())) {
            return new MultiLineJsonRecordReader(clazz);
        } else {
            return new SingleLineJsonRecordReader(clazz);
        }
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        boolean multiline = acceptMultilineJson(context.getConfiguration());
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return ((null == codec) || (codec instanceof SplittableCompressionCodec))
                && !multiline;
    }

    private boolean acceptMultilineJson(Configuration configuration) {
        return configuration.getBoolean(JSON_MULTILINE, true);
    }

    private static class SingleLineJsonRecordReader extends RecordReader<NullWritable, Object> {

        private final LineRecordReader reader;
        private final Function<? super String, Object> mapper;

        SingleLineJsonRecordReader(Class<?> clazz) {
            mapper = mapper(clazz);
            reader = new LineRecordReader();
        }

        private static FunctionEx<? super String, Object> mapper(Class<?> clazz) {
            return clazz == null
                    ? JsonUtil::mapFrom
                    : line -> JsonUtil.beanFrom(line, clazz);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            reader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            return reader.nextKeyValue();
        }

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public Object getCurrentValue() {
            return mapper.apply(reader.getCurrentValue().toString());
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static class MultiLineJsonRecordReader extends RecordReader<NullWritable, Object> {

        private final Class<?> clazz;
        private InputStreamReader reader;
        private Iterator<?> iterator;
        private boolean processed;
        private Object current;

        MultiLineJsonRecordReader(Class<?> clazz) {
            this.clazz = clazz;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            FileSplit fileSplit = (FileSplit) split;
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            FSDataInputStream in = fs.open(file);

            // Jackson doesn't handle empty files
            if (hasNoData(in)) {
                in.close();
                iterator = emptyIterator();
                return;
            }

            reader = new InputStreamReader(in, StandardCharsets.UTF_8);
            if (clazz == null) {
                iterator = JsonUtil.mapSequenceFrom(reader);
            } else {
                iterator = JsonUtil.beanSequenceFrom(reader, clazz);
            }
        }

        private boolean hasNoData(FSDataInputStream in) throws IOException {
            // We could use org.apache.hadoop.fs.FileSystem.getFileStatus() to get length, but it results in an extra
            // call to the server in case of remote filesystems
            // Instead we just try to read a single byte and the buffered data is used later.
            long startPosition = in.getPos();
            boolean hasNoData = in.read() == -1;
            in.seek(startPosition);
            return hasNoData;
        }

        @Override
        public boolean nextKeyValue() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            } else {
                processed = true;
                return false;
            }
        }

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public Object getCurrentValue() {
            return current;
        }

        @Override
        public float getProgress() {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
