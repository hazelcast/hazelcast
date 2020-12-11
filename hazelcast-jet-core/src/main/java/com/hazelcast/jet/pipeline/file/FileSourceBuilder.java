/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.file;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.file.impl.FileSourceConfiguration;
import com.hazelcast.jet.pipeline.file.impl.FileSourceFactory;
import com.hazelcast.jet.pipeline.file.impl.LocalFileSourceFactory;

import javax.annotation.Nonnull;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * A unified builder object for various kinds of file sources. It works
 * with the local filesystem and several distributed filesystems
 * supported through the Hadoop API.
 * <p>
 * The builder requires two parameters: {@code path} and {@code format},
 * and creates a {@link BatchSource}. The path specifies the filesystem
 * type (examples: {@code s3a://}, {@code hdfs://}) and the path to the
 * file.
 * <p>
 * The format determines how Jet will map the contents of the file to the
 * objects coming out of the corresponding pipeline source. This involves
 * two major concerns: parsing and deserialization. For example, {@link
 * LinesTextFileFormat} parses the file into lines of text and emits a
 * simple string for each line; {@link JsonFileFormat} parses a JSON Lines
 * file and emits instances of the class you specify.
 * <p>
 * You may also use Hadoop to read local files by specifying the
 * {@link #useHadoopForLocalFiles(boolean)} flag.
 * <p>
 * Usage:
 * <pre>{@code
 * BatchSource<User> source = new FileSourceBuilder("data/users.jsonl")
 *   .withFormat(new JsonFileFormat<>(User.class))
 *   .build();
 * }</pre>
 *
 * @param <T> type of items a source using this file format will emit
 * @since 4.4
 */
public class FileSourceBuilder<T> {

    private static final List<String> HADOOP_PREFIXES;

    static {
        HADOOP_PREFIXES = Collections.unmodifiableList(asList(
                "s3a://",   // Amazon S3
                "hdfs://",  // HDFS
                "wasbs://", // Azure Cloud Storage
                "adl://",   // Azure Data Lake Gen 1
                "abfs://",   // Azure Data Lake Gen 2
                "gs://"     // Google Cloud Storage
        ));
    }

    private final Map<String, String> options = new HashMap<>();

    private final String path;
    private String glob = "*";
    private FileFormat<T> format;
    private boolean useHadoop;
    private boolean sharedFileSystem;

    /**
     * Creates a new file source builder with the given path. The path
     * must point to a directory. All files in the directory are
     * processed. The directory is not processed recursively.
     *
     * @param path path pointing to a directory to read files from
     */
    public FileSourceBuilder(@Nonnull String path) {
        this.path = requireNonNull(path, "path must not be null");
        if (!(Paths.get(path).isAbsolute() || hasHadoopPrefix(path))) {
            throw new IllegalArgumentException("Provided path must be absolute. path: " + path);
        }
    }

    /**
     * Sets a glob pattern to filter the files in the specified directory.
     * The default value is '*', matching all files in the directory.
     *
     * @param glob glob pattern,
     */
    public FileSourceBuilder<T> glob(@Nonnull String glob) {
        this.glob = requireNonNull(glob, "glob must not be null");
        return this;
    }

    /**
     * Set the file format for the source. Currently supported file formats are:
     * <ul>
     * <li> {@link AvroFileFormat}
     * <li> {@link CsvFileFormat}
     * <li> {@link JsonFileFormat}
     * <li> {@link LinesTextFileFormat}
     * <li> {@link ParquetFileFormat}
     * <li> {@link RawBytesFileFormat}
     * <li> {@link TextFileFormat}
     * </ul>
     * You may provide a custom format by implementing the {@link FileFormat}
     * interface. See its javadoc for details.
     */
    @Nonnull
    public <T_NEW> FileSourceBuilder<T_NEW> format(@Nonnull FileFormat<T_NEW> fileFormat) {
        @SuppressWarnings("unchecked")
        FileSourceBuilder<T_NEW> newThis = (FileSourceBuilder<T_NEW>) this;
        newThis.format = fileFormat;
        return newThis;
    }

    /**
     * Specifies to use Hadoop for files from local filesystem. One advantage
     * of Hadoop is that it can provide better parallelization when the number
     * of files is smaller than the total parallelism of the pipeline source.
     * Defaults to false.
     *
     * @param useHadoop if Hadoop should be use for reading local filesystem
     */
    @Nonnull
    public FileSourceBuilder<T> useHadoopForLocalFiles(boolean useHadoop) {
        this.useHadoop = useHadoop;
        return this;
    }

    /**
     * Sets if files are in a shared storage visible to all members. Default
     * value is {@code false}.
     * <p>
     * If {@code sharedFileSystem} is {@code true}, Jet will assume all members
     * see the same files. They will split the work so that each member will
     * read a part of the files. If {@code sharedFileSystem} is {@code false},
     * each member will read all files in the directory, assuming the are
     * local.
     * <p>
     * If you start all the members on a single machine (such as for
     * development), set this property to true. If you have multiple machines
     * with multiple members each and the directory is not a shared storage,
     * it's not possible to configure the file reader correctly - use only one
     * member per machine.
     * <p>
     * NOTE: Only valid for local filesystem, distributed filesystems are
     * always shared.
     */
    @Nonnull
    public FileSourceBuilder<T> sharedFileSystem(boolean sharedFileSystem) {
        this.sharedFileSystem = sharedFileSystem;
        return this;
    }

    /**
     * Specifies an arbitrary option for the underlying source. If you are
     * looking for a missing option, check out the {@link FileFormat} class
     * you're using, it offers parsing-related options.
     */
    @Nonnull
    public FileSourceBuilder<T> option(String key, String value) {
        requireNonNull(key, "key must not be null");
        requireNonNull(value, "value must not be null");
        options.put(key, value);
        return this;
    }

    /**
     * Builds a {@link BatchSource} based on the current state of the builder.
     */
    @Nonnull
    public BatchSource<T> build() {
        ProcessorMetaSupplier metaSupplier = buildMetaSupplier();

        return Sources.batchFromProcessor("files(path=" + path + ", glob=" + glob + ", hadoop=" + shouldUseHadoop(),
                metaSupplier);
    }

    /**
     * Builds a {@link com.hazelcast.jet.core.ProcessorMetaSupplier} based on the current state of the builder.
     */
    @Nonnull
    public ProcessorMetaSupplier buildMetaSupplier() {
        if (path == null) {
            throw new IllegalStateException("Parameter 'path' is required");
        }
        if (format == null) {
            throw new IllegalStateException("Parameter 'format' is required");
        }

        FileSourceConfiguration<T> fsc = new FileSourceConfiguration<>(
                path, glob, format, sharedFileSystem, options
        );

        if (shouldUseHadoop()) {
            ServiceLoader<FileSourceFactory> loader = ServiceLoader.load(FileSourceFactory.class);
            // Only one implementation is expected to be present on classpath
            Iterator<FileSourceFactory> iterator = loader.iterator();
            if (!iterator.hasNext()) {
                throw new JetException("No suitable FileSourceFactory found. " +
                                       "Do you have Jet's Hadoop module on classpath?");
            }
            FileSourceFactory fileSourceFactory = iterator.next();
            if (iterator.hasNext()) {
                throw new JetException("Multiple FileSourceFactory implementations found");
            }
            return fileSourceFactory.create(fsc);
        }
        return new LocalFileSourceFactory().create(fsc);
    }

    private boolean shouldUseHadoop() {
        return useHadoop || hasHadoopPrefix(path);
    }

    private static boolean hasHadoopPrefix(String path) {
        return HADOOP_PREFIXES.stream().anyMatch(path::startsWith);
    }
}
