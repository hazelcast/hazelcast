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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

public class CsvInputFormat extends FileInputFormat<NullWritable, Object> {

    public static final String CSV_INPUT_FORMAT_BEAN_CLASS = "csv.bean.class";

    @Override
    public RecordReader<NullWritable, Object> createRecordReader(InputSplit split, TaskAttemptContext context) {

        return new RecordReader<NullWritable, Object>() {

            private Object current;
            private MappingIterator<Object> iterator;

            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

                FileSplit fileSplit = (FileSplit) split;
                Configuration conf = context.getConfiguration();

                Configuration configuration = context.getConfiguration();
                String className = configuration.get(CSV_INPUT_FORMAT_BEAN_CLASS);
                Class<?> clazz = ReflectionUtils.loadClass(className);

                CsvMapper mapper = new CsvMapper();

                CsvSchema schema = CsvSchema.emptySchema().withHeader();
                ObjectReader reader = mapper.readerFor(clazz)
                                            .withoutFeatures(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                                            .with(schema);

                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = fs.open(file);
                iterator = reader.readValues((InputStream) in);
            }

            @Override
            public boolean nextKeyValue() {
                if (!iterator.hasNext()) {
                    return false;
                }
                current = iterator.next();
                return true;
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
                return 0;
            }

            @Override
            public void close() throws IOException {
                iterator.close();
            }
        };
    }
}
