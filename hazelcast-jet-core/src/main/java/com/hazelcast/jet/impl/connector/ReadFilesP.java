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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;

/**
 * Private API, use {@link SourceProcessors#readFilesP}.
 * <p>
 * Since the work of this vertex is file IO-intensive, its {@link
 * com.hazelcast.jet.core.Vertex#localParallelism(int) local parallelism}
 * should be set according to the performance characteristics of the
 * underlying storage system. Modern high-end devices peak with 4-8 reading
 * threads, so if running a single Jet job with a single file-reading
 * vertex, the optimal value would be in the range of 4-8. Note that any
 * one file is only read by one thread, so extra parallelism won't improve
 * performance if there aren't enough files to read.
 */
public final class ReadFilesP<W, R> extends AbstractProcessor {

    private final Path directory;
    private final String glob;
    private final boolean sharedFileSystem;
    private final DistributedFunction<Path, Stream<W>> readFileFn;
    private final DistributedBiFunction<String, W, R> mapOutputFn;

    private int processorIndex;
    private int parallelism;
    private DirectoryStream<Path> directoryStream;
    private Traverser<R> outputTraverser;
    private Stream<W> currentStream;

    private ReadFilesP(@Nonnull String directory, @Nonnull String glob, boolean sharedFileSystem,
                       @Nonnull DistributedFunction<Path, Stream<W>> readFileFn,
                       @Nonnull DistributedBiFunction<String, W, R> mapOutputFn) {
        this.directory = Paths.get(directory);
        this.glob = glob;
        this.readFileFn = readFileFn;
        this.mapOutputFn = mapOutputFn;
        this.sharedFileSystem = sharedFileSystem;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        processorIndex = sharedFileSystem ? context.globalProcessorIndex() : context.localProcessorIndex();
        parallelism = sharedFileSystem ? context.totalParallelism() : context.localParallelism();

        directoryStream = Files.newDirectoryStream(directory, glob);
        outputTraverser = Traversers.traverseIterator(directoryStream.iterator())
                                    .filter(this::shouldProcessEvent)
                                    .flatMap(this::processFile);
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(outputTraverser);
    }

    private boolean shouldProcessEvent(Path file) {
        if (Files.isDirectory(file)) {
            return false;
        }
        int hashCode = file.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == processorIndex;
    }

    private Traverser<R> processFile(Path file) {
        if (getLogger().isFinestEnabled()) {
            getLogger().finest("Processing file " + file);
        }
        assert currentStream == null : "currentStream != null";
        currentStream = readFileFn.apply(file);
        String fileName = file.getFileName().toString();
        return traverseStream(currentStream)
                .map(line -> mapOutputFn.apply(fileName, line))
                .onFirstNull(() -> {
                    currentStream.close();
                    currentStream = null;
                });
    }

    @Override
    public void close(@Nullable Throwable error) throws IOException {
        IOException ex = null;
        if (directoryStream != null) {
            try {
                directoryStream.close();
            } catch (IOException e) {
                ex = e;
            }
        }
        if (currentStream != null) {
            currentStream.close();
        }
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Private API. Use {@link SourceProcessors#readFilesP} instead.
     */
    public static <W, R> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull DistributedFunction<Path, Stream<W>> readFileFn,
            @Nonnull DistributedBiFunction<String, W, R> mapOutputFn
    ) {
        return ProcessorMetaSupplier.of(() -> new ReadFilesP<>(directory, glob, sharedFileSystem, readFileFn, mapOutputFn),
                2);
    }
}
