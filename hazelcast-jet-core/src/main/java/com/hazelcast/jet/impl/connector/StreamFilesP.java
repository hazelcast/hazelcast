/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.sun.nio.file.SensitivityWatchEventModifier.HIGH;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @see com.hazelcast.jet.Processors#streamFiles(String, Charset, String)
 */
public class StreamFilesP extends AbstractProcessor implements Closeable {

    /**
     * Number of lines read in one batch, to allow for timely draining of watcher
     * events even while reading a file to avoid
     * {@link java.nio.file.StandardWatchEventKinds#OVERFLOW OVERFLOW}.
     */
    private static final int LINES_IN_ONE_BATCH = 64;

    // exposed for testing
    final Map<Path, Long> fileOffsets = new HashMap<>();

    private final Path watchedDirectory;
    private final Charset charset;
    private final PathMatcher glob;
    private final int parallelism;

    private final int id;
    private final Queue<Path> eventQueue = new ArrayDeque<>();

    private WatchService watcher;
    private StringBuilder lineBuilder = new StringBuilder();
    private Path currentFile;
    private FileInputStream currentInputStream;
    private Reader currentReader;

    StreamFilesP(@Nonnull String watchedDirectory, @Nonnull Charset charset, @Nonnull String glob,
                 int parallelism, int id
    ) {
        this.watchedDirectory = Paths.get(watchedDirectory);
        this.charset = charset;
        this.glob = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        this.parallelism = parallelism;
        this.id = id;
        setCooperative(false);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        for (Path file : Files.newDirectoryStream(watchedDirectory)) {
            if (Files.isRegularFile(file)) {
                // Negative offset means "initial offset", needed to skip the first line
                fileOffsets.put(file, -Files.size(file));
            }
        }
        watcher = FileSystems.getDefault().newWatchService();
        watchedDirectory.register(watcher, new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE}, HIGH);
        getLogger().info("Started to watch directory: " + watchedDirectory);
    }

    @Override
    public void close() {
        try {
            closeCurrentFile();
            if (isClosed()) {
                return;
            }
            getLogger().info("Closing StreamFilesP. Any pending watch events will be processed.");
            watcher.close();
        } catch (IOException e) {
            getLogger().severe("Failed to close StreamFilesP", e);
        } finally {
            watcher = null;
        }
    }

    @Override
    public boolean complete() {
        try {
            if (!isClosed()) {
                drainWatcherEvents();
            } else if (eventQueue.isEmpty()) {
                return true;
            }
            if (currentFile == null) {
                currentFile = eventQueue.poll();
            }
            if (currentFile != null) {
                processFile();
            }
            return false;
        } catch (InterruptedException e) {
            close();
            return true;
        }
    }

    private void drainWatcherEvents() throws InterruptedException {
        final ILogger logger = getLogger();
        // poll with blocking only when there is no other work to do
        final WatchKey key = (currentFile == null && eventQueue.isEmpty())
                ? watcher.poll(1, SECONDS)
                : watcher.poll();
        if (key == null) {
            if (!Files.exists(watchedDirectory)) {
                logger.info("Directory " + watchedDirectory + " does not exist, stopped watching");
                close();
            }
            return;
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
            close();
        }
    }

    private boolean belongsToThisProcessor(Path path) {
        return ((path.hashCode() & Integer.MAX_VALUE) % parallelism) == id;
    }

    private void processFile() {
        try {
            if (!ensureFileOpen()) {
                return;
            }
            for (int i = 0; i < LINES_IN_ONE_BATCH; i++) {
                String line = readCompleteLine(currentReader);
                if (line == null) {
                    fileOffsets.put(currentFile, currentInputStream.getChannel().position());
                    closeCurrentFile();
                    break;
                }
                emit(line);
            }
        } catch (IOException e) {
            close();
            throw sneakyThrow(e);
        }
    }

    private boolean ensureFileOpen() throws IOException {
        if (currentReader != null) {
            return true;
        }
        long offset = fileOffsets.getOrDefault(currentFile, 0L);
        logFinest(getLogger(), "Processing file %s, previous offset: %,d", currentFile, offset);
        try {
            FileInputStream fis = new FileInputStream(currentFile.toFile());
            // Negative offset means we're reading the file for the first time.
            // We recover the actual offset by negating, then we subtract one
            // so as not to miss a preceding newline.
            fis.getChannel().position(offset >= 0 ? offset : -offset - 1);
            BufferedReader r = new BufferedReader(new InputStreamReader(fis, charset));
            if (offset < 0 && !findNextLine(r, offset)) {
                closeCurrentFile();
                return false;
            }
            currentReader = r;
            currentInputStream = fis;
            return true;
        } catch (FileNotFoundException ignored) {
            // This could be caused by ENTRY_MODIFY emitted on file deletion
            // just before ENTRY_DELETE
            closeCurrentFile();
            return false;
        }
    }

    private boolean findNextLine(Reader in, long offset) throws IOException {
        while (true) {
            int ch = in.read();
            if (ch < 0) {
                // we've hit EOF before finding the end of current line
                fileOffsets.put(currentFile, offset);
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
            if (ch < 0) {
                break;
            }
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
        currentReader = null;
        currentInputStream = null;
    }

    private boolean isClosed() {
        return watcher == null;
    }

    /**
     * @see com.hazelcast.jet.Processors#streamFiles(String, Charset, String)
     */
    public static ProcessorSupplier supplier(@Nonnull String watchedDirectory, @Nonnull String charset,
                                             @Nonnull String glob
    ) {
        return new Supplier(watchedDirectory, charset, glob);
    }

    private static final class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;
        private final String watchedDirectory;
        private final String charset;
        private final String glob;

        private transient ArrayList<StreamFilesP> processors;

        private Supplier(String watchedDirectory, String charset, String glob) {
            this.watchedDirectory = watchedDirectory;
            this.charset = charset;
            this.glob = glob;
        }

        @Override @Nonnull
        public List<StreamFilesP> get(int count) {
            processors = new ArrayList<>(count);
            Charset charsetObj = Charset.forName(charset);
            for (int i = 0; i < count; i++) {
                processors.add(new StreamFilesP(watchedDirectory, charsetObj, glob, count, i));
            }
            return processors;
        }

        @Override
        public void complete(Throwable error) {
            processors.forEach(r -> uncheckRun(r::close));
        }
    }
}
