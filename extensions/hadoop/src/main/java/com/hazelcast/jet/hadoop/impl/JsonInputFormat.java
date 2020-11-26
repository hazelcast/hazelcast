/*
 * Copyright 2020 Hazelcast Inc.
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

import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.json.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class JsonInputFormat extends FileInputFormat<LongWritable, Object> {

    public static final String JSON_INPUT_FORMAT_BEAN_CLASS = "json.bean.class";

    @Override
    public RecordReader<LongWritable, Object> createRecordReader(InputSplit split, TaskAttemptContext context) {

        Configuration configuration = context.getConfiguration();
        String className = configuration.get(JSON_INPUT_FORMAT_BEAN_CLASS);
        Class<?> clazz = ReflectionUtils.loadClass(className);

        return new RecordReader<LongWritable, Object>() {

            final LineRecordReader reader = new LineRecordReader();

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
                reader.initialize(split, context);
            }

            @Override
            public boolean nextKeyValue() throws IOException {
                return reader.nextKeyValue();
            }

            @Override
            public LongWritable getCurrentKey() {
                return reader.getCurrentKey();
            }

            @Override
            public Object getCurrentValue() throws IOException {
                return JsonUtil.beanFrom(reader.getCurrentValue().toString(), clazz);
            }

            @Override
            public float getProgress() throws IOException {
                return reader.getProgress();
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return null == codec || codec instanceof SplittableCompressionCodec;
    }
}
