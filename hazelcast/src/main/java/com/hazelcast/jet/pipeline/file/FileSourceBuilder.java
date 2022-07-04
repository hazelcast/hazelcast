/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.jet.pipeline.file.WildcardMatcher.hasWildcard;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * A unified builder object for various kinds of file sources.
 * <p>
 * To create an instance, use {@link FileSources#files(String)}.
 *
 * @param <T> the type of items a source using this file format will emit
 * @since Jet 4.4
 */
public class FileSourceBuilder<T> {

    private static final List<String> HADOOP_PREFIXES = Collections.unmodifiableList(asList(
            "s3a://",   // Amazon S3
            "hdfs://",  // HDFS
            "wasbs://", // Azure Cloud Storage
            "adl://",   // Azure Data Lake Gen 1
            "abfs://",   // Azure Data Lake Gen 2
            "gs://"     // Google Cloud Storage
    ));

    private final Map<String, String> options = new HashMap<>();

    private final String path;
    private String glob = "*";
    private FileFormat<T> format;
    private boolean useHadoop;
    private boolean sharedFileSystem;
    private boolean ignoreFileNotFound;

    FileSourceBuilder(@Nonnull String path) {
        this.path = requireNonNull(path, "path must not be null");
        if (hasWildcard(path)) {
            throw new IllegalArgumentException("Provided path must not contain any wildcard characters, path: " + path);
        }
        if (!(hasHadoopPrefix(path) || Paths.get(path).isAbsolute())) {
            throw new IllegalArgumentException("Provided path must be absolute, path: " + path);
        }
    }

    /**
     * Sets a glob pattern to filter the files in the specified directory. The
     * default value is '*', matching all files in the directory.
     *
     * @param glob glob pattern,
     */
    public FileSourceBuilder<T> glob(@Nonnull String glob) {
        this.glob = requireNonNull(glob, "glob must not be null");
        return this;
    }

    /**
     * Set the file format for the source. See {@link FileFormat} for available
     * formats and factory methods.
     * <p>
     * It's not possible to implement a custom format.
     */
    @Nonnull
    public <T_NEW> FileSourceBuilder<T_NEW> format(@Nonnull FileFormat<T_NEW> fileFormat) {
        @SuppressWarnings("unchecked")
        FileSourceBuilder<T_NEW> newThis = (FileSourceBuilder<T_NEW>) this;
        newThis.format = fileFormat;
        return newThis;
    }

    /**
     * Specifies that Jet should use Apache Hadoop for files from the local
     * filesystem. Otherwise, local files are read by Jet directly. One
     * advantage of Hadoop is that it can provide better parallelization when
     * the number of files is smaller than the total parallelism of the
     * pipeline source.
     * <p>
     * Default value is {@code false}.
     *
     * @param useHadoop if Hadoop should be use for reading local filesystem
     */
    @Nonnull
    public FileSourceBuilder<T> useHadoopForLocalFiles(boolean useHadoop) {
        this.useHadoop = useHadoop;
        return this;
    }

    /**
     * If {@code sharedFileSystem} is {@code true}, Jet will assume all members
     * see the same files. They will split the work so that each member will
     * read a part of the files. If {@code sharedFileSystem} is {@code false},
     * each member will read all files in the directory, assuming that other
     * members see different files.
     * <p>
     * This option applies only for the local filesystem when {@linkplain
     * #useHadoopForLocalFiles(boolean) Hadoop is not used} and when the
     * directory doesn't contain a prefix for a remote file system. Distributed
     * filesystems are always assumed to be shared.
     * <p>
     * If you start all the members on a single machine (such as for
     * development), set this property to {@code true}. If you have multiple
     * machines with multiple members each and the directory is not a shared
     * storage, it's not possible to configure the file reader correctly - use
     * only one member per machine.
     * <p>
     * Default value is {@code false}.
     */
    @Nonnull
    public FileSourceBuilder<T> sharedFileSystem(boolean sharedFileSystem) {
        this.sharedFileSystem = sharedFileSystem;
        return this;
    }

    /**
     * Set to true to ignore no matching files in the directory specified by
     * {@code path}.
     * <p>
     * When there is no file matching the glob specified by
     * {@link #glob(String)} (or the default glob) Jet throws an exception by
     * default. This might be problematic in some cases, where the directory
     * is empty. To override this behaviour set this to true.
     * <p>
     * If set to true and there are no files in the directory the source will
     * produce 0 items.
     * <p>
     * Default value is {@code false}.
     *
     * @param ignoreFileNotFound true if no files in the specified directory should be accepted
     */
    @Nonnull
    public FileSourceBuilder<T> ignoreFileNotFound(boolean ignoreFileNotFound) {
        this.ignoreFileNotFound = ignoreFileNotFound;
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
     * Builds a {@link ProcessorMetaSupplier} based on the current state of the
     * builder. Use for integration with the Core API.
     * <p>
     * This method is a part of Core API and has lower backward-compatibility
     * guarantees (we can change it in minor version).
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
                path, glob, format, sharedFileSystem, ignoreFileNotFound, options
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

    /**
     * Checks if the given path starts with one of the defined Hadoop
     * prefixes:
     *     "s3a://",   // Amazon S3
     *     "hdfs://",  // HDFS
     *     "wasbs://", // Azure Cloud Storage
     *     "adl://",   // Azure Data Lake Gen 1
     *     "abfs://",  // Azure Data Lake Gen 2
     *     "gs://"     // Google Cloud Storage
     *
     * see {@link #HADOOP_PREFIXES}
     */
    public static boolean hasHadoopPrefix(String path) {
        return HADOOP_PREFIXES.stream().anyMatch(path::startsWith);
    }
}
