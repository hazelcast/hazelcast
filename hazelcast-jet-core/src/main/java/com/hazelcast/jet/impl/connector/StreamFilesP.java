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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Private API. Access via {@link
 * com.hazelcast.jet.core.processor.SourceProcessors#streamFilesP}.
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
public class StreamFilesP<R> extends AbstractProcessor {

    /**
     * The amount of data read from one file at once must be limited
     * in order to prevent a possible {@link java.nio.file.StandardWatchEventKinds#OVERFLOW
     * OVERFLOW} if too many Watcher events accumulate in the queue. This
     * constant specifies the number of lines to read at once, before going
     * back to polling the event queue.
     */
    private static final int LINES_IN_ONE_BATCH = 64;
    private static final String SENSITIVITY_MODIFIER_CLASS_NAME = "com.sun.nio.file.SensitivityWatchEventModifier";
    private static final WatchEvent.Kind[] WATCH_EVENT_KINDS = {ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE};
    private static final WatchEvent.Modifier[] WATCH_EVENT_MODIFIERS = getHighSensitivityModifiers();

    /**
     * Map from file to offset. Initially we store (-fileSize): if the offset is negative when we
     * receive the first watcher event, we skip up to the next newline to avoid partial reading
     * of the first line.
     */
    // exposed for testing
    final Map<Path, FileOffset> fileOffsets = new HashMap<>();

    private final Path watchedDirectory;
    private final Charset charset;
    private final PathMatcher glob;
    private final boolean sharedFileSystem;
    private final BiFunctionEx<? super String, ? super String, ? extends R> mapOutputFn;

    private final Queue<Path> eventQueue = new ArrayDeque<>();

    private WatchService watcher;
    private StringBuilder lineBuilder = new StringBuilder();
    private R pendingItem;
    private Path currentFile;
    private String currentFileName;
    private FileInputStream currentInputStream;
    private Reader currentReader;
    private int parallelism;
    private int processorIndex;

    StreamFilesP(
            @Nonnull String watchedDirectory,
            @Nonnull Charset charset,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull BiFunctionEx<? super String, ? super String, ? extends R> mapOutputFn
    ) {
        this.watchedDirectory = Paths.get(watchedDirectory);
        this.charset = charset;
        this.glob = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        this.sharedFileSystem = sharedFileSystem;
        this.mapOutputFn = mapOutputFn;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        processorIndex = sharedFileSystem ? context.globalProcessorIndex() : context.localProcessorIndex();
        parallelism = sharedFileSystem ? context.totalParallelism() : context.localParallelism();

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(watchedDirectory)) {
            for (Path file : directoryStream) {
                if (Files.isRegularFile(file)) {
                    // Negative offset means "initial offset", needed to skip the first line
                    fileOffsets.put(file, new FileOffset(-Files.size(file), ""));
                }
            }
        }
        watcher = FileSystems.getDefault().newWatchService();
        watchedDirectory.register(watcher, WATCH_EVENT_KINDS, WATCH_EVENT_MODIFIERS);
        getLogger().info("Started to watch directory: " + watchedDirectory);
    }

    @Override
    public void close() {
        try {
            closeCurrentFile();
            getLogger().fine("Closing StreamFilesP");
            watcher.close();
        } catch (IOException e) {
            getLogger().severe("Failed to close StreamFilesP", e);
        } finally {
            watcher = null;
        }
    }

    @Override
    public boolean complete() {
        if (!drainWatcherEvents()) {
            return true;
        }
        if (currentFile == null) {
            currentFile = eventQueue.poll();
            currentFileName = currentFile != null ? String.valueOf(currentFile.getFileName()) : null;
        }
        if (currentFile != null) {
            processFile();
        }
        return false;
    }

    /**
     * @return false, if the watcher should be closed
     */
    private boolean drainWatcherEvents() {
        final ILogger logger = getLogger();
        // poll with blocking only when there is no other work to do
        final WatchKey key;
        try {
            key = (currentFile == null && eventQueue.isEmpty())
                    ? watcher.poll(1, SECONDS)
                    : watcher.poll();
        } catch (InterruptedException e) {
            return false;
        }
        if (key == null) {
            if (!Files.exists(watchedDirectory)) {
                logger.info("Directory " + watchedDirectory + " does not exist, stopped watching");
                return false;
            }
            return true;
        }
        for (WatchEvent<?> event : key.pollEvents()) {
            final WatchEvent.Kind<?> kind = event.kind();
            final Path fileName = ((WatchEvent<Path>) event).context();
            final Path filePath = watchedDirectory.resolve(fileName);
            if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                if (glob.matches(fileName) && belongsToThisProcessor(fileName) && !Files.isDirectory(filePath)) {
                    logFine(logger, "Will open file to read new content: %s", filePath);
                    eventQueue.add(filePath);
                }
            } else if (kind == ENTRY_DELETE) {
                logFinest(logger, "File was deleted: %s", filePath);
                fileOffsets.remove(filePath);
            } else if (kind == OVERFLOW) {
                logger.warning("Detected OVERFLOW in " + watchedDirectory);
            } else {
                throw new JetException("Unknown kind of WatchEvent: " + kind);
            }
        }
        if (!key.reset()) {
            logger.info("Watch key is invalid. Stopping watcher.");
            return false;
        }
        return true;
    }

    private boolean belongsToThisProcessor(Path path) {
        return ((path.hashCode() & Integer.MAX_VALUE) % parallelism) == processorIndex;
    }

    private void processFile() {
        try {
            if (!ensureFileOpen()) {
                return;
            }
            for (int i = 0; i < LINES_IN_ONE_BATCH; i++) {
                if (pendingItem == null) {
                    String line = readCompleteLine(currentReader);
                    pendingItem = line != null ? mapOutputFn.apply(currentFileName, line) : null;
                }
                if (pendingItem == null) {
                    fileOffsets.put(currentFile,
                            new FileOffset(currentInputStream.getChannel().position(), lineBuilder.toString()));
                    lineBuilder.setLength(0);
                    closeCurrentFile();
                    break;
                }
                if (tryEmit(pendingItem)) {
                    pendingItem = null;
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    private boolean ensureFileOpen() throws IOException {
        if (currentReader != null) {
            return true;
        }
        FileOffset offset = fileOffsets.getOrDefault(currentFile, FileOffset.ZERO);
        logFine(getLogger(), "Processing file %s, previous offset: %s", currentFile, offset);
        try {
            FileInputStream fis = new FileInputStream(currentFile.toFile());
            fis.getChannel().position(offset.positiveOffset());
            BufferedReader r = new BufferedReader(new InputStreamReader(fis, charset));
            if (offset.offset < 0 && !findEndOfLine(r)) {
                closeCurrentFile();
                return false;
            }
            currentReader = r;
            currentInputStream = fis;
            lineBuilder.append(offset.pendingLine);
            return true;
        } catch (FileNotFoundException ignored) {
            // This could be caused by ENTRY_MODIFY emitted on file deletion
            // just before ENTRY_DELETE
            closeCurrentFile();
            return false;
        }
    }

    /**
     * Searches for the end-of-line marker in the input stream.
     *
     * @return whether the end-of-line marker was found
     */
    private static boolean findEndOfLine(Reader in) throws IOException {
        while (true) {
            int ch = in.read();
            if (ch < 0) {
                // we've hit EOF before finding the end of current line
                return false;
            }
            if (ch == '\n' || ch == '\r') {
                maybeSkipLF(in, ch);
                return true;
            }
        }
    }

    /**
     * Reads a line from the input only if it is terminated by CR or LF or
     * CRLF. If it detects EOF before the newline character, returns
     * {@code null}.
     *
     * @return The line (possibly zero-length) or null on EOF.
     */
    // package-visible for testing
    String readCompleteLine(Reader reader) throws IOException {
        int ch;
        while ((ch = reader.read()) >= 0) {
            if (ch == '\r' || ch == '\n') {
                maybeSkipLF(reader, ch);
                try {
                    return lineBuilder.toString();
                } finally {
                    lineBuilder.setLength(0);
                }
            } else {
                lineBuilder.append((char) ch);
            }
        }
        // EOF
        return null;
    }

    private static void maybeSkipLF(Reader reader, int ch) throws IOException {
        // look ahead for possible '\n' after '\r' (windows end-line style)
        if (ch == '\r') {
            reader.mark(1);
            int ch2 = reader.read();
            if (ch2 != '\n') {
                reader.reset();
            }
        }
    }

    private void closeCurrentFile() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        }
        currentFile = null;
        currentFileName = null;
        currentReader = null;
        currentInputStream = null;
    }

    /**
     * Private API. Use {@link
     * com.hazelcast.jet.core.processor.SourceProcessors#streamFilesP} instead.
     */
    @Nonnull
    public static ProcessorMetaSupplier metaSupplier(
            @Nonnull String watchedDirectory,
            @Nonnull String charset,
            @Nonnull String glob,
            boolean sharedFileSystem,
            @Nonnull BiFunctionEx<? super String, ? super String, ?> mapOutputFn
    ) {
        checkSerializable(mapOutputFn, "mapOutputFn");

        return ProcessorMetaSupplier.of(2, () ->
                new StreamFilesP<>(watchedDirectory, Charset.forName(charset), glob, sharedFileSystem, mapOutputFn));
    }

    private static WatchEvent.Modifier[] getHighSensitivityModifiers() {
        // Modifiers for file watch service to achieve the highest possible sensitivity.
        // Background: Java 7 SE defines no standard modifiers for a watch service. However some JDKs use internal
        // modifiers to increase sensitivity. This field contains modifiers to be used for highest possible sensitivity.
        // It's JVM-specific and hence it's just a best-effort.
        // I believe this is useful on platforms without native watch service (or where Java does not use it) e.g. MacOSX
        Object modifier = ReflectionUtils.readStaticFieldOrNull(SENSITIVITY_MODIFIER_CLASS_NAME, "HIGH");
        if (modifier instanceof WatchEvent.Modifier) {
            return new WatchEvent.Modifier[]{(WatchEvent.Modifier) modifier};
        }
        //bad luck, we did not find the modifier
        return new WatchEvent.Modifier[0];
    }

    private static final class FileOffset {
        private static final FileOffset ZERO = new FileOffset(0, "");

        private final long offset;
        private final String pendingLine;

        private FileOffset(long offset, @Nonnull String pendingLine) {
            this.offset = offset;
            this.pendingLine = pendingLine;
        }

        /**
         * Negative offset means we're reading the file for the first time.
         * We recover the actual offset by negating, then we subtract one
         * so that we don't skip the first line if we started right after a newline.
         */
        private long positiveOffset() {
            return offset >= 0 ? offset : -offset - 1;
        }

        @Override
        public String toString() {
            return "FileOffset{offset=" + offset + ", pendingLine='" + pendingLine + '\'' + '}';
        }
    }
}
