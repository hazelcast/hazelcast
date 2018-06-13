/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.connector.ReadFilesP;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Builder for a file source which reads lines from files in a directory (but not
 * its subdirectories) and emits output object created by {@code mapOutputFn}
 *
 * @param <W> the type of the stream created by {@code readFileFn},
 *           default value is {@linkplain String}
 * @param <R> the type of the items the source emits
 */
public final class FileSourceBuilder<W, R> {

    private static final String GLOB_WILDCARD = "*";

    private final String directory;

    private Charset charset = UTF_8;
    private String glob = GLOB_WILDCARD;
    private boolean sharedFileSystem;
    private String sourceName = "filesSource";
    private DistributedFunction<Path, Stream<W>> readFileFn;
    private DistributedBiFunction<String, W, ? extends R> mapOutputFn = (file, line) -> (R) line;

    /**
     * Use {@link Sources#filesBuilder}.
     */
    FileSourceBuilder(@Nonnull String directory) {
        this.directory = directory;
    }

    /**
     * This constructor is used by Avro module.
     */
    public FileSourceBuilder(@Nonnull String directory,
                             @Nonnull String sourceName,
                             @Nonnull DistributedFunction<Path, Stream<W>> readFileFn) {
        this.directory = directory;
        this.sourceName = sourceName;
        this.readFileFn = readFileFn;
    }

    /**
     * Sets the character set used to encode the files. Default value is {@link
     * java.nio.charset.StandardCharsets#UTF_8}.
     * <p>
     * Setting this component does not have any effect if builder is used by
     * Avro module.
     */
    public FileSourceBuilder<W, R> charset(@Nonnull Charset charset) {
        this.charset = charset;
        return this;
    }

    /**
     * Sets the globbing mask, see {@link
     * java.nio.file.FileSystem#getPathMatcher(String) getPathMatcher()}.
     * Default value is {@code "*"} which means all files.
     */
    public FileSourceBuilder<W, R> glob(@Nonnull String glob) {
        this.glob = glob;
        return this;
    }

    /**
     * Sets if files are in a shared storage visible to all members. Default
     * value is {@code false}
     * <p>
     * If {@code sharedFileSystem} is {@code true}, Jet will assume all members
     * see the same files. They will split the work so that each member will
     * read a part of the files. If {@code sharedFileSystem} is {@code false},
     * each member will read all files in the directory, assuming the are
     * local.
     */
    public FileSourceBuilder<W, R> sharedFileSystem(boolean sharedFileSystem) {
        this.sharedFileSystem = sharedFileSystem;
        return this;
    }

    /**
     * Sets the function which creates output object from each line. Default value
     * is {@code (file, line) -> line}.
     */
    public FileSourceBuilder<W, R> mapOutputFn(@Nonnull DistributedBiFunction<String, W, ? extends R> mapOutputFn) {
        this.mapOutputFn = mapOutputFn;
        return this;
    }

    /**
     * Builds a custom file {@link BatchSource} with supplied components.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * Any {@code IOException} will cause the job to fail. The files must not
     * change while being read; if they do, the behavior is unspecified.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     */
    public BatchSource<R> build() {
        if (readFileFn == null) {
            String charsetName = charset.name();
            readFileFn = path -> uncheckCall(() -> (Stream<W>) Files.lines(path, Charset.forName(charsetName)));
        }
        return batchFromProcessor(sourceName + "(" + new File(directory, glob) + ')',
                ReadFilesP.metaSupplier(directory, glob, sharedFileSystem, readFileFn, mapOutputFn));
    }

    /**
     * Builds a source that emits a stream of lines of text coming from files in
     * the watched directory (but not its subdirectories). It will emit only
     * new contents added after startup: both new files and new content
     * appended to existing ones.
     * <p>
     * If, during the scanning phase, the source observes a file that doesn't
     * end with a newline, it will assume that there is a line just being
     * written. This line won't appear in its output.
     * <p>
     * The source completes when the directory is deleted. However, in order
     * to delete the directory, all files in it must be deleted and if you
     * delete a file that is currently being read from, the job may encounter
     * an {@code IOException}. The directory must be deleted on all nodes if
     * {@code sharedFileSystem} is {@code false}.
     * <p>
     * Any {@code IOException} will cause the job to fail.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * lines added after the restart will be emitted, which gives at-most-once
     * behavior.
     * <p>
     * The default local parallelism for this processor is 2 (or 1 if just 1
     * CPU is available).
     *
     * <h3>Limitation on Windows</h3>
     * On Windows the {@code WatchService} is not notified of appended lines
     * until the file is closed. If the file-writing process keeps the file
     * open while appending, the processor may fail to observe the changes.
     * It will be notified if any process tries to open that file, such as
     * looking at the file in Explorer. This holds for Windows 10 with the NTFS
     * file system and might change in future. You are advised to do your own
     * testing on your target Windows platform.
     *
     * <h3>Use the latest JRE</h3>
     * The underlying JDK API ({@link java.nio.file.WatchService}) has a
     * history of unreliability and this source may experience infinite
     * blocking, missed, or duplicate events as a result. Such problems may be
     * resolved by upgrading the JRE to the latest version.
     */
    public StreamSource<R> buildWatcher() {
        DistributedBiFunction<String, String, ? extends R> outputFn = (DistributedBiFunction) mapOutputFn;
        return Sources.streamFromProcessor("fileWatcherSource(" + directory + '/' + glob + ')',
                SourceProcessors.streamFilesP(directory, charset, glob, sharedFileSystem, outputFn));
    }
}
