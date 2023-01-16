/*
 * Copyright 2023 Hazelcast Inc.
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
import com.fasterxml.jackson.dataformat.csv.CsvParser.Feature;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.Util.createFieldProjection;
import static java.util.function.Function.identity;

public class CsvInputFormat extends FileInputFormat<NullWritable, Object> {

    public static final String CSV_INPUT_FORMAT_BEAN_CLASS = "csv.bean.class";
    public static final String CSV_INPUT_FORMAT_FIELD_LIST_PREFIX = "csv.field.list.";

    @Override
    public RecordReader<NullWritable, Object> createRecordReader(InputSplit split, TaskAttemptContext context) {

        return new RecordReader<NullWritable, Object>() {

            private Class<?> formatClazz;
            private final LineRecordReader reader = new LineRecordReader();

            private ObjectReader objectReader;
            private Function<Object, Object> projection = identity();

            @SuppressWarnings({"unchecked", "rawtypes"})
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
                reader.initialize(split, context);

                FileSplit fileSplit = (FileSplit) split;
                if (fileSplit.getStart() == 0) {
                    // We always expect a header, which we read below,
                    // if this split is the first in the file, skip the header
                    reader.nextKeyValue();
                }

                Configuration configuration = context.getConfiguration();
                String className = configuration.get(CSV_INPUT_FORMAT_BEAN_CLASS);
                formatClazz = className == null ? null : ReflectionUtils.loadClass(className);
                String[] header = readHeader(fileSplit, context);
                if (formatClazz == String[].class) {
                    objectReader = new CsvMapper()
                            .enable(Feature.WRAP_AS_ARRAY)
                            .readerFor(String[].class)
                            .with(CsvSchema.emptySchema().withSkipFirstDataRow(false));

                    List<String> fieldNames = new ArrayList<>();
                    String field;
                    for (int i = 0; (field = configuration.get(CSV_INPUT_FORMAT_FIELD_LIST_PREFIX + i)) != null; i++) {
                        fieldNames.add(field);
                    }

                    projection = (Function) createFieldProjection(header, fieldNames);
                } else {
                    Builder builder = CsvSchema.builder();
                    for (String column : header) {
                        builder.addColumn(column);
                    }
                    objectReader = new CsvMapper().readerFor(formatClazz)
                                                  .withoutFeatures(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                                                  .with(builder.build());
                }
            }

            private String[] readHeader(FileSplit fileSplit, TaskAttemptContext context) throws IOException {
                Configuration configuration = context.getConfiguration();
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(configuration);

                ObjectReader headerReader = new CsvMapper()
                        .enable(Feature.WRAP_AS_ARRAY)
                        .readerFor(String[].class)
                        .with(CsvSchema.emptySchema().withSkipFirstDataRow(false));

                try (InputStream in = fs.open(file);
                     InputStream wrapped = wrap(in, configuration, file);
                     MappingIterator<Object> iterator = headerReader.readValues(wrapped)) {

                    if (!iterator.hasNext()) {
                        throw new JetException("Header row missing in " + fileSplit);
                    }
                    return (String[]) iterator.next();
                }

            }

            private InputStream wrap(InputStream in, Configuration configuration, Path file) throws IOException {
                CompressionCodec codec = new CompressionCodecFactory(configuration).getCodec(file);
                return codec != null ? codec.createInputStream(in) : in;
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
            public Object getCurrentValue() throws IOException {
                String current = reader.getCurrentValue().toString();
                if (formatClazz == String[].class) {
                    try (MappingIterator<Object> iterator = objectReader.readValues(current)) {
                        return iterator.hasNext() ? projection.apply(iterator.next()) : null;
                    }
                } else {
                    return current.isEmpty() ? null : projection.apply(objectReader.readValue(current));
                }
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
        return ((null == codec) || (codec instanceof SplittableCompressionCodec));
    }
}
