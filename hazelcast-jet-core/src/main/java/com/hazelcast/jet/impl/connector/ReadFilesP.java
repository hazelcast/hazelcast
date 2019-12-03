/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

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
public final class ReadFilesP<R, T> extends AbstractProcessor {

    private final Path directory;
    private final String glob;
    private final boolean sharedFileSystem;
    private final FunctionEx<? super Path, ? extends Stream<R>> readFileFn;
    private final BiFunctionEx<? super String, ? super R, ? extends T> mapOutputFn;

    private int processorIndex;
    private int parallelism;
    private DirectoryStream<Path> directoryStream;
    private Traverser<? extends T> outputTraverser;
    private Stream<R> currentStream;

    private ReadFilesP(
            @Nonnull String directory,
            @Nonnull String glob, boolean sharedFileSystem,
            @Nonnull FunctionEx<? super Path, ? extends Stream<R>> readFileFn,
            @Nonnull BiFunctionEx<? super String, ? super R, ? extends T> mapOutputFn
    ) {
        this.directory = Paths.get(directory);
        this.glob = glob;
        this.readFileFn = readFileFn;
        this.mapOutputFn = mapOutputFn;
        this.sharedFileSystem = sharedFileSystem;
    }

    @Override
    public boolean isCooperative() {
        return false;
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

    private Traverser<? extends T> processFile(Path file) {
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
    public void close() throws IOException {
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

    /**
     * Private API. Use {@link SourceProcessors#readFilesP} instead.
     */
    public static <W, T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull FunctionEx<? super Path, ? extends Stream<W>> readFileFn,
            @Nonnull BiFunctionEx<? super String, ? super W, ? extends T> mapOutputFn
    ) {
        checkSerializable(readFileFn, "readFileFn");
        checkSerializable(mapOutputFn, "mapOutputFn");

        return ProcessorMetaSupplier.of(2, () -> new ReadFilesP<>(
                directory, glob, sharedFileSystem, readFileFn, mapOutputFn)
        );
    }
}
