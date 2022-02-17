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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.pipeline.file.impl.FileProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.file.impl.FileTraverser;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.permission.ConnectorPermission;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Permission;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterator;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;

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
public final class ReadFilesP<T> extends AbstractProcessor {

    private static final int DEFAULT_LOCAL_PARALLELISM = 4;

    private final String directory;
    private final String glob;
    private final boolean sharedFileSystem;
    private final boolean ignoreFileNotFound;
    private final FunctionEx<? super Path, ? extends Stream<T>> readFileFn;

    private LocalFileTraverser<T> traverser;

    private ReadFilesP(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            boolean ignoreFileNotFound,
            @Nonnull FunctionEx<? super Path, ? extends Stream<T>> readFileFn
    ) {
        this.directory = directory;
        this.glob = glob;
        this.sharedFileSystem = sharedFileSystem;
        this.ignoreFileNotFound = ignoreFileNotFound;
        this.readFileFn = readFileFn;
    }

    @Override
    protected void init(@Nonnull Context context) {
        ILogger logger = context.logger();
        int processorIndex = sharedFileSystem ? context.globalProcessorIndex() : context.localProcessorIndex();
        int parallelism = sharedFileSystem ? context.totalParallelism() : context.localParallelism();

        traverser = new LocalFileTraverser<>(
                logger,
                directory,
                glob,
                ignoreFileNotFound,
                path -> shouldProcessEvent(path, parallelism, processorIndex),
                readFileFn
        );
    }

    private static boolean shouldProcessEvent(Path path, int parallelism, int processorIndex) {
        int hashCode = path.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == processorIndex;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    @Override
    public void close() throws IOException {
        if (traverser != null) {
            traverser.close();
        }
    }

    /**
     * Private API. Use {@link SourceProcessors#readFilesP} instead.
     */
    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directory,
            @Nonnull String glob,
            boolean sharedFileSystem,
            boolean ignoreFileNotFound,
            @Nonnull FunctionEx<? super Path, ? extends Stream<T>> readFileFn
    ) {
        checkSerializable(readFileFn, "readFileFn");

        return new MetaSupplier<>(DEFAULT_LOCAL_PARALLELISM, directory, glob, sharedFileSystem,
                ignoreFileNotFound, readFileFn);
    }

    private static final class MetaSupplier<T> implements FileProcessorMetaSupplier<T> {

        private static final long serialVersionUID = 1L;

        private static final ILogger LOGGER = Logger.getLogger(MetaSupplier.class);

        private final int localParallelism;
        private final String directory;
        private final String glob;
        private final boolean sharedFileSystem;
        private final boolean ignoreFileNotFound;
        private final FunctionEx<? super Path, ? extends Stream<T>> readFileFn;

        private MetaSupplier(
                int localParallelism,
                String directory,
                String glob,
                boolean sharedFileSystem,
                boolean ignoreFileNotFound,
                FunctionEx<? super Path, ? extends Stream<T>> readFileFn
        ) {
            this.localParallelism = localParallelism;
            this.directory = directory;
            this.glob = glob;
            this.sharedFileSystem = sharedFileSystem;
            this.ignoreFileNotFound = ignoreFileNotFound;
            this.readFileFn = readFileFn;
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> ProcessorSupplier.of(() -> new ReadFilesP<>(directory, glob, sharedFileSystem,
                    ignoreFileNotFound, readFileFn));
        }

        @Override
        public int preferredLocalParallelism() {
            return localParallelism;
        }

        @Override
        public FileTraverser<T> traverser() {
            return new LocalFileTraverser<>(LOGGER, directory, glob, ignoreFileNotFound, path -> true, readFileFn);
        }

        @Override
        public Permission getRequiredPermission() {
            return ConnectorPermission.file(directory, ACTION_READ);
        }
    }

    private static final class LocalFileTraverser<T> implements FileTraverser<T> {

        private final ILogger logger;
        private final Path directory;
        private final String glob;
        private final boolean ignoreFileNotFound;
        private final FunctionEx<? super Path, ? extends Stream<T>> readFileFn;
        private final Traverser<T> delegate;

        private DirectoryStream<Path> directoryStream;
        private Stream<T> fileStream;
        private boolean hasResults;

        private LocalFileTraverser(
                ILogger logger,
                String directory,
                String glob,
                boolean ignoreFileNotFound,
                Predicate<Path> pathFilterFn,
                FunctionEx<? super Path, ? extends Stream<T>> readFileFn
        ) {
            this.logger = logger;
            this.directory = Paths.get(directory);
            this.glob = glob;
            this.ignoreFileNotFound = ignoreFileNotFound;
            this.readFileFn = readFileFn;
            this.delegate = traverseIterator(uncheckCall(this::paths))
                    .filter(path -> !Files.isDirectory(path))
                    .peek(path -> hasResults = true)
                    .filter(path -> pathFilterFn.test(path))
                    .flatMap(this::processFile);
        }

        private Iterator<Path> paths() throws IOException {
            File file = directory.toFile();
            if (!file.exists()) {
                throw new JetException("The directory '" + directory + "' does not exist.");
            }
            if (!file.isDirectory()) {
                throw new JetException("The given path (" + directory + ") must point to a directory, not a file.");
            }

            directoryStream = Files.newDirectoryStream(directory, glob);
            return directoryStream.iterator();
        }

        private Traverser<T> processFile(Path file) {
            logger.finest("Processing file " + file);

            assert fileStream == null : "fileStream != null";
            fileStream = readFileFn.apply(file);
            return traverseStream(fileStream)
                    .onFirstNull(() -> {
                        fileStream.close();
                        fileStream = null;
                    });
        }

        @Override
        public T next() {
            T next = delegate.next();
            if (next == null && !hasResults && !ignoreFileNotFound) {
                throw new JetException("The glob " + glob + " matches no files in directory " + directory);
            }
            return next;
        }

        @Override
        public void close() throws IOException {
            IOException exception = null;
            if (directoryStream != null) {
                try {
                    directoryStream.close();
                } catch (IOException ioe) {
                    exception = ioe;
                }
            }
            if (fileStream != null) {
                fileStream.close();
            }
            if (exception != null) {
                throw exception;
            }
        }
    }
}
