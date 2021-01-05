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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.file.AvroFileFormat;
import com.hazelcast.jet.pipeline.file.CsvFileFormat;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.JsonFileFormat;
import com.hazelcast.jet.pipeline.file.LinesTextFileFormat;
import com.hazelcast.jet.pipeline.file.ParquetFileFormat;
import com.hazelcast.jet.pipeline.file.RawBytesFileFormat;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import com.hazelcast.jet.pipeline.file.impl.FileSourceConfiguration;
import com.hazelcast.jet.pipeline.file.impl.FileSourceFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import static com.hazelcast.jet.hadoop.HadoopProcessors.readHadoopP;
import static com.hazelcast.jet.hadoop.HadoopSources.COPY_ON_READ;
import static com.hazelcast.jet.hadoop.impl.CsvInputFormat.CSV_INPUT_FORMAT_BEAN_CLASS;
import static com.hazelcast.jet.hadoop.impl.JsonInputFormat.JSON_INPUT_FORMAT_BEAN_CLASS;
import static java.util.Objects.requireNonNull;

/**
 * Hadoop-based implementation of {@link FileSourceFactory}.
 */
public class HadoopFileSourceFactory implements FileSourceFactory {

    private final Map<String, JobConfigurer> configurers;

    /**
     * Creates the HadoopSourceFactory.
     */
    public HadoopFileSourceFactory() {
        configurers = new HashMap<>();

        addJobConfigurer(configurers, new AvroFormatJobConfigurer());
        addJobConfigurer(configurers, new CsvFormatJobConfigurer());
        addJobConfigurer(configurers, new JsonFormatJobConfigurer());
        addJobConfigurer(configurers, new LineTextJobConfigurer());
        addJobConfigurer(configurers, new ParquetFormatJobConfigurer());
        addJobConfigurer(configurers, new RawBytesFormatJobConfigurer());
        addJobConfigurer(configurers, new TextJobConfigurer());

        ServiceLoader<JobConfigurer> loader = ServiceLoader.load(JobConfigurer.class);
        for (JobConfigurer jobConfigurer : loader) {
            addJobConfigurer(configurers, jobConfigurer);
        }
    }

    private static void addJobConfigurer(Map<String, JobConfigurer> configurers, JobConfigurer configurer) {
        configurers.put(configurer.format(), configurer);
    }

    @Nonnull
    @Override
    public <T> ProcessorMetaSupplier create(@Nonnull FileSourceConfiguration<T> fsc) {
        try {
            Job job = Job.getInstance();

            Configuration configuration = job.getConfiguration();
            configuration.setBoolean(FileInputFormat.INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, true);
            configuration.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, false);
            configuration.setBoolean(HadoopSources.SHARED_LOCAL_FS, fsc.isSharedFileSystem());
            for (Entry<String, String> option : fsc.getOptions().entrySet()) {
                configuration.set(option.getKey(), option.getValue());
            }

            Path inputPath = getInputPath(fsc);
            FileInputFormat.addInputPath(job, inputPath);

            FileFormat<T> fileFormat = requireNonNull(fsc.getFormat());
            JobConfigurer configurer = this.configurers.get(fileFormat.format());
            if (configurer == null) {
                throw new JetException("Could not find JobConfigurer for FileFormat: " + fileFormat.format() + ". " +
                        "Did you provide correct modules on classpath?");
            }
            configurer.configure(job, fileFormat);

            return readHadoopP(SerializableConfiguration.asSerializable(configuration), configurer.projectionFn());
        } catch (IOException e) {
            throw new JetException("Could not create a source", e);
        }
    }

    @Nonnull
    private <T> Path getInputPath(FileSourceConfiguration<T> fsc) {
        if (fsc.getGlob().equals("*")) {
            // * means all files in the directory, but also all directories
            // Hadoop interprets it as multiple input folders, resulting to processing files in 1st level
            // subdirectories
            return new Path(fsc.getPath());
        } else {
            return new Path(fsc.getPath() + File.separatorChar + fsc.getGlob());
        }
    }

    private static class AvroFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            AvroFileFormat<T> avroFileFormat = (AvroFileFormat<T>) format;
            job.setInputFormatClass(AvroKeyInputFormat.class);

            Class<?> reflectClass = avroFileFormat.reflectClass();
            if (reflectClass != null) {
                Schema schema = ReflectData.get().getSchema(reflectClass);
                AvroJob.setInputKeySchema(job, schema);
            } else {
                job.getConfiguration().setBoolean(COPY_ON_READ, Boolean.FALSE);
            }
        }

        @Override
        public BiFunctionEx<AvroKey<?>, NullWritable, ?> projectionFn() {
            return (k, v) -> {
                Object record = k.datum();
                return record instanceof GenericContainer ? copy((GenericContainer) record) : record;
            };
        }

        @Nonnull
        @Override
        public String format() {
            return AvroFileFormat.FORMAT_AVRO;
        }
    }

    private static class RawBytesFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(WholeFileAsBytesInputFormat.class);
        }

        @Override
        public BiFunctionEx<NullWritable, BytesWritable, byte[]> projectionFn() {
            return (k, v) -> v.copyBytes();
        }

        @Nonnull
        @Override
        public String format() {
            return RawBytesFileFormat.FORMAT_BIN;
        }
    }

    private static class CsvFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            CsvFileFormat<T> csvFileFormat = (CsvFileFormat<T>) format;
            job.setInputFormatClass(CsvInputFormat.class);
            job.getConfiguration().setBoolean(COPY_ON_READ, Boolean.FALSE);

            Class<?> clazz = csvFileFormat.clazz();
            if (clazz != null) {
                job.getConfiguration().set(CSV_INPUT_FORMAT_BEAN_CLASS, clazz.getCanonicalName());
            }
        }

        @Override
        public BiFunctionEx<NullWritable, ?, ?> projectionFn() {
            return (k, v) -> v;
        }

        @Nonnull
        @Override
        public String format() {
            return CsvFileFormat.FORMAT_CSV;
        }
    }

    private static class JsonFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            JsonFileFormat<T> jsonFileFormat = (JsonFileFormat<T>) format;
            job.setInputFormatClass(JsonInputFormat.class);
            job.getConfiguration().setBoolean(COPY_ON_READ, Boolean.FALSE);

            Class<?> clazz = jsonFileFormat.clazz();
            if (clazz != null) {
                job.getConfiguration().set(JSON_INPUT_FORMAT_BEAN_CLASS, clazz.getCanonicalName());
            }
        }

        @Override
        public BiFunctionEx<LongWritable, ?, ?> projectionFn() {
            return (k, v) -> v;
        }

        @Nonnull
        @Override
        public String format() {
            return JsonFileFormat.FORMAT_JSON;
        }
    }

    private static class LineTextJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(TextInputFormat.class);
        }

        @Override
        public BiFunctionEx<LongWritable, Text, String> projectionFn() {
            return (k, v) -> v.toString();
        }

        @Nonnull
        @Override
        public String format() {
            return LinesTextFileFormat.FORMAT_LINES;
        }
    }

    private static class ParquetFormatJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(AvroParquetInputFormat.class);
            job.getConfiguration().setBoolean(COPY_ON_READ, Boolean.FALSE);
        }

        @Override
        public BiFunctionEx<String, ?, ?> projectionFn() {
            return (k, record) -> {
                if (record == null) {
                    return null;
                } else if (record instanceof GenericContainer) {
                    return copy((GenericContainer) record);
                } else {
                    throw new IllegalArgumentException("Unexpected record type: " + record.getClass());
                }
            };
        }

        @Nonnull
        @Override
        public String format() {
            return ParquetFileFormat.FORMAT_PARQUET;
        }
    }

    private static class TextJobConfigurer implements JobConfigurer {

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            job.setInputFormatClass(WholeFileAsTextInputFormat.class);
        }

        @Override
        public BiFunctionEx<NullWritable, Text, String> projectionFn() {
            return (k, v) -> v.toString();
        }

        @Nonnull
        @Override
        public String format() {
            return TextFileFormat.FORMAT_TXT;
        }
    }

    /**
     * Copies Avro record.
     */
    @SuppressWarnings("unchecked")
    private static <T extends GenericContainer> T copy(T record) {
        if (record instanceof SpecificRecord) {
            SpecificRecord specificRecord = (SpecificRecord) record;
            return (T) SpecificData.get().deepCopy(specificRecord.getSchema(), specificRecord);
        } else if (record instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord) record;
            return (T) GenericData.get().deepCopy(genericRecord.getSchema(), genericRecord);
        } else {
            throw new IllegalArgumentException("Unexpected record type: " + record.getClass());
        }
    }
}
