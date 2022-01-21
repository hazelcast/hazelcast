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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
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
import com.hazelcast.security.permission.ConnectorPermission;
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
import org.apache.hadoop.fs.FileSystem;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.Permission;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import static com.hazelcast.jet.hadoop.HadoopProcessors.readHadoopP;
import static com.hazelcast.jet.hadoop.HadoopSources.COPY_ON_READ;
import static com.hazelcast.jet.hadoop.impl.CsvInputFormat.CSV_INPUT_FORMAT_BEAN_CLASS;
import static com.hazelcast.jet.hadoop.impl.CsvInputFormat.CSV_INPUT_FORMAT_FIELD_LIST_PREFIX;
import static com.hazelcast.jet.hadoop.impl.JsonInputFormat.JSON_INPUT_FORMAT_BEAN_CLASS;
import static com.hazelcast.jet.hadoop.impl.JsonInputFormat.JSON_MULTILINE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Hadoop-based implementation of {@link FileSourceFactory}.
 */
public class HadoopFileSourceFactory implements FileSourceFactory {

    private final Map<String, JobConfigurer> configurers = new HashMap<>();

    /**
     * Creates the HadoopSourceFactory.
     */
    public HadoopFileSourceFactory() {

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
        FileFormat<T> fileFormat = requireNonNull(fsc.getFormat());
        JobConfigurer configurer = configurers.get(fileFormat.format());
        if (configurer == null) {
            throw new JetException("Could not find JobConfigurer for FileFormat: " + fileFormat.format() + ". " +
                    "Did you provide correct modules on classpath?");
        }

        return readHadoopP(
                ConnectorPermission.file(fsc.getPath(), ACTION_READ),
                configureFn(fsc, configurer, fileFormat),
                configurer.projectionFn()
        );
    }

    private static <T> ConsumerEx<Configuration> configureFn(
            FileSourceConfiguration<T> fsc, JobConfigurer configurer, FileFormat<T> fileFormat) {
        return new ConsumerEx<Configuration>() {

            @Override
            public void acceptEx(Configuration configuration) throws Exception {
                try {
                    configuration.setBoolean(FileInputFormat.INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, true);
                    configuration.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, false);
                    configuration.setBoolean(HadoopSources.SHARED_LOCAL_FS, fsc.isSharedFileSystem());
                    configuration.setBoolean(HadoopSources.IGNORE_FILE_NOT_FOUND, fsc.isIgnoreFileNotFound());
                    for (Entry<String, String> option : fsc.getOptions().entrySet()) {
                        configuration.set(option.getKey(), option.getValue());
                    }

                    // Some methods we use to configure actually take a Job
                    Job job = Job.getInstance(configuration);
                    Path inputPath = getInputPath(fsc, configuration);
                    FileInputFormat.addInputPath(job, inputPath);

                    configurer.configure(job, fileFormat);

                    // The job creates a copy of the configuration, so we need to copy the new setting to the
                    // original configuration instance
                    for (Entry<String, String> entry : job.getConfiguration()) {
                        configuration.set(entry.getKey(), entry.getValue());
                    }
                } catch (IOException e) {
                    throw new JetException("Could not create a source", e);
                }
            }

            @Override
            public List<Permission> permissions() {
                String keyFile = fsc.getOptions().get("google.cloud.auth.service.account.json.keyfile");
                if (keyFile != null) {
                    return asList(ConnectorPermission.file(keyFile, ACTION_READ),
                            ConnectorPermission.file(fsc.getPath(), ACTION_READ));
                }
                return singletonList(ConnectorPermission.file(fsc.getPath(), ACTION_READ));
            }
        };
    }

    @Nonnull
    private static <T> Path getInputPath(FileSourceConfiguration<T> fsc, Configuration configuration)
            throws IOException {

        // validate that fsc.getPath() is a directory, not a file to keep same behavior as local file connector and
        // to avoid surprises for the user
        Path path = new Path(fsc.getPath());
        try {
            FileSystem fs = path.getFileSystem(configuration);
            if (!fs.getFileStatus(path).isDirectory()) {
                throw new JetException("The given path (" + path + ") must point to a directory, not a file.");
            }
        } catch (FileNotFoundException e) {
            throw new JetException("The directory '" + path + "' does not exist.");
        }

        if (fsc.getGlob().equals("*")) {
            // * means all files in the directory, but also all directories
            // Hadoop interprets it as multiple input folders, resulting to processing files in 1st level
            // subdirectories
            return new Path(fsc.getPath());
        } else {
            return new Path(fsc.getPath(), fsc.getGlob());
        }
    }

    private static class AvroFormatJobConfigurer implements JobConfigurer {

        private static final long serialVersionUID = 1L;

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

        private static final long serialVersionUID = 1L;

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

        private static final long serialVersionUID = 1L;

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            CsvFileFormat<T> csvFileFormat = (CsvFileFormat<T>) format;
            job.setInputFormatClass(CsvInputFormat.class);
            job.getConfiguration().setBoolean(COPY_ON_READ, Boolean.FALSE);
            job.getConfiguration().set(CSV_INPUT_FORMAT_BEAN_CLASS, csvFileFormat.clazz().getName());
            List<String> fieldList = csvFileFormat.fieldNames();
            if (fieldList != null) {
                for (int i = 0; i < fieldList.size(); i++) {
                    job.getConfiguration().set(CSV_INPUT_FORMAT_FIELD_LIST_PREFIX + i, fieldList.get(i));
                }
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

        private static final long serialVersionUID = 1L;

        @Override
        public <T> void configure(Job job, FileFormat<T> format) {
            JsonFileFormat<T> jsonFileFormat = (JsonFileFormat<T>) format;
            job.setInputFormatClass(JsonInputFormat.class);

            Configuration configuration = job.getConfiguration();
            configuration.setBoolean(COPY_ON_READ, Boolean.FALSE);
            configuration.setBoolean(JSON_MULTILINE, jsonFileFormat.isMultiline());
            Class<?> clazz = jsonFileFormat.clazz();
            if (clazz != null) {
                configuration.set(JSON_INPUT_FORMAT_BEAN_CLASS, clazz.getName());
            }
        }

        @Override
        public BiFunctionEx<NullWritable, ?, ?> projectionFn() {
            return (k, v) -> v;
        }

        @Nonnull
        @Override
        public String format() {
            return JsonFileFormat.FORMAT_JSON;
        }
    }

    private static class LineTextJobConfigurer implements JobConfigurer {

        private static final long serialVersionUID = 1L;

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

        private static final long serialVersionUID = 1L;

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

        private static final long serialVersionUID = 1L;

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
