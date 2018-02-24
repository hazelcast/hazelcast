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
import com.hazelcast.jet.core.CloseableProcessorSupplier;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.function.DistributedBiFunction;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.stream.Collectors.toList;

/**
 * Private API, use {@link SourceProcessors#readFilesP(String, Charset, String)}.
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
public final class ReadFilesP<R> extends AbstractProcessor implements Closeable {

    private final Charset charset;
    private final int parallelism;
    private final int id;
    private final DistributedBiFunction<String, String, R> mapOutputFn;
    private final Path directory;
    private final String glob;
    private DirectoryStream<Path> directoryStream;
    private Traverser<R> outputTraverser;
    private Stream<String> currentFileLines;

    private ReadFilesP(String directory, Charset charset, String glob, int parallelism, int id,
                       DistributedBiFunction<String, String, R> mapOutputFn) {
        this.directory = Paths.get(directory);
        this.glob = glob;
        this.charset = charset;
        this.parallelism = parallelism;
        this.id = id;
        this.mapOutputFn = mapOutputFn;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        directoryStream = Files.newDirectoryStream(directory, glob);
        outputTraverser = Traversers.traverseIterator(directoryStream.iterator())
                                    .filter(this::shouldProcessEvent)
                                    .flatMap(this::processFile)
                                    .onFirstNull(() -> {
                                        uncheckRun(this::close);
                                        directoryStream = null;
                                    });
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
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == id;
    }

    private Traverser<R> processFile(Path file) {
        if (getLogger().isFinestEnabled()) {
            getLogger().finest("Processing file " + file);
        }
        try {
            assert currentFileLines == null : "currentFileLines != null";
            currentFileLines = Files.lines(file, charset);
            String fileName = file.getFileName().toString();
            return traverseStream(currentFileLines)
                    .map(line -> mapOutputFn.apply(fileName, line))
                    .onFirstNull(() -> {
                        currentFileLines.close();
                        currentFileLines = null;
                    });
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
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
        if (currentFileLines != null) {
            currentFileLines.close();
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
    public static ProcessorMetaSupplier metaSupplier(
            @Nonnull String directory,
            @Nonnull String charset,
            @Nonnull String glob,
            @Nonnull DistributedBiFunction<String, String, ?> mapOutputFn
    ) {
        return ProcessorMetaSupplier.of(new CloseableProcessorSupplier<>(
                count -> IntStream.range(0, count)
                                  .mapToObj(i -> new ReadFilesP(directory, Charset.forName(charset), glob, count, i,
                                          mapOutputFn))
                                  .collect(toList())),
                2);
    }
}
