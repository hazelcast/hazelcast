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
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.sun.nio.file.SensitivityWatchEventModifier.HIGH;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @see com.hazelcast.jet.Processors#streamFiles(String, Charset)
 */
public class StreamFilesP extends AbstractProcessor implements Closeable {

    /**
     * Number of lines read in one batch, to allow for timely draining of watcher
     * events even while reading a file to avoid
     * {@link java.nio.file.StandardWatchEventKinds#OVERFLOW OVERFLOW}.
     */
    private static final int LINES_IN_ONE_BATCH = 64;

    private final Charset charset;
    private final int parallelism;
    private final int id;
    private final Path watchedDirectory;

    private final Map<Path, Long> fileOffsets = new HashMap<>();

    private WatchService watcher;
    private StringBuilder lineBuilder = new StringBuilder();
    private Path currentFile;
    private FileInputStream currentInputStream;
    private Reader currentReader;

    StreamFilesP(String watchedDirectory, Charset charset, int parallelism, int id) {
        this.watchedDirectory = Paths.get(watchedDirectory);
        this.charset = charset;
        this.parallelism = parallelism;
        this.id = id;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        for (Path file : Files.newDirectoryStream(watchedDirectory)) {
            if (Files.isRegularFile(file)) {
                // I insert negative size to indicate, that we should skip the first line
                fileOffsets.put(file, -Files.size(file));
            }
        }
    }

    @Override
    public boolean complete() {
        try {
            watcher = FileSystems.getDefault().newWatchService();
            watchedDirectory.register(watcher, new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE}, HIGH);
            getLogger().info("Started to watch the directory: " + watchedDirectory);

            ArrayDeque<Path> eventQueue = new ArrayDeque<>();
            while (watcher != null || !eventQueue.isEmpty()) {
                if (watcher != null) {
                    drainWatcherEvents(eventQueue);
                }
                if (currentFile == null) {
                    currentFile = eventQueue.poll();
                }
                if (currentFile != null) {
                    processFile();
                }
            }

            close();

            return true;
        } catch (IOException e) {
            throw sneakyThrow(e);
        } catch (InterruptedException e) {
            return true;
        }
    }

    private void drainWatcherEvents(ArrayDeque<Path> eventQueue) throws InterruptedException, IOException {
        WatchKey key;
        if (currentFile == null && eventQueue.isEmpty()) {
            // poll with blocking only when there is no other work to do
            key = watcher.poll(3, SECONDS);
        } else {
            // if there are more lines to emit from current file, just poll without waiting
            key = watcher.poll();
        }

        if (key == null) {
            if (!Files.exists(watchedDirectory)) {
                getLogger().info("Directory " + watchedDirectory + " does not exist, stopped watching");
                close();
            }
            return;
        }

        for (WatchEvent<?> event : key.pollEvents()) {
            final WatchEvent.Kind<?> kind = event.kind();
            final Path filePath = watchedDirectory.resolve(((WatchEvent<Path>) event).context());

            if (kind == OVERFLOW) {
                getLogger().warning("Detected OVERFLOW in " + watchedDirectory);
                continue;
            }

            if (kind == ENTRY_DELETE && filePath.equals(watchedDirectory)) {
                // we'll probably never receive this event, but who knows
                getLogger().info("Directory " + watchedDirectory + " deleted, stopped watching");
                close();
                return;
            }

            if (kind == ENTRY_DELETE) {
                if (getLogger().isFineEnabled()) {
                    getLogger().fine("Detected deleted file: " + filePath);
                }
                fileOffsets.remove(filePath);
                continue;
            }

            assert kind == ENTRY_CREATE || kind == ENTRY_MODIFY;
            if (getLogger().isFineEnabled()) {
                getLogger().fine("Detected new or modified file: " + filePath);
            }

            // ignore subdirectories
            if (Files.isDirectory(filePath)) {
                continue;
            }

            if (shouldProcessEvent(filePath)) {
                eventQueue.add(filePath);
            }
        }
        if (!key.reset()) {
            getLogger().info("Watch key is invalid. Stopping watcher.");
            close();
        }
    }

    private boolean shouldProcessEvent(Path file) {
        int hashCode = file.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == id;
    }

    private void processFile() throws IOException {
        try {
            // if file is not opened, open it
            if (currentInputStream == null) {
                long offset = fileOffsets.getOrDefault(currentFile, 0L);
                if (getLogger().isFinestEnabled()) {
                    getLogger().finest("Processing file " + currentFile + ", previous offset: " + offset);
                }

                try {
                    currentInputStream = new FileInputStream(currentFile.toFile());
                } catch (FileNotFoundException ignored) {
                    // ignore IO errors: on file deletion, ENTRY_MODIFY is reported just before ENTRY_DELETE
                    closeCurrentFile();
                    return;
                }
                // if offset is negative, it means, that we are reading the file for the first time
                boolean findNextLine = false;
                final long originalOffset = offset;
                if (offset < 0) {
                    // -1: we start one byte behind, to see, if there is a newline.
                    // On windows, we'll miss the CR from CRLF sequence, but that's not a problem
                    // because LF is a line terminator also.
                    offset = -offset - 1;
                    findNextLine = true;
                }

                currentInputStream.getChannel().position(offset);
                currentReader = new BufferedReader(new InputStreamReader(currentInputStream, charset));
                if (findNextLine) {
                    int ch;
                    while (true) {
                        ch = currentReader.read();
                        if (ch < 0) {
                            // we've hit EOF before finding the end of current line
                            fileOffsets.put(currentFile, originalOffset);
                            closeCurrentFile();
                            return;
                        }
                        if (ch == '\n' || ch == '\r') {
                            maybeSkipLF(currentReader, ch);
                            break;
                        }
                    }
                }
            }

            // now read regular lines
            String line;
            for (int i = 0; i < LINES_IN_ONE_BATCH; i++) {
                line = readCompleteLine(currentReader);
                if (line == null) {
                    fileOffsets.put(currentFile, currentInputStream.getChannel().position());
                    closeCurrentFile();
                    break;
                } else {
                    emit(line);
                }
            }
        } catch (IOException e) {
            // just log the exception. We don't want the streaming job to fail
            getLogger().warning(e);
            closeCurrentFile();
        }
    }

    /**
     * Reads a line from the input, only if it is terminated by CR or LF or CRLF. If EOF is
     * detected before newline character, returns null.
     *
     * @return The line (possibly zero-length) or null on EOF.
     */
    // package-visible for test
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

    private void maybeSkipLF(Reader reader, int ch) throws IOException {
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
                // just ignore the exception
                getLogger().warning(e);
            }
        }
        currentFile = null;
        currentReader = null;
        currentInputStream = null;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public void close() throws IOException {
        closeCurrentFile();
        if (watcher != null) {
            getLogger().info("Closing watcher");
            watcher.close();
            watcher = null;
        }
    }

    /**
     * @see com.hazelcast.jet.Processors#streamFiles(String, Charset)
     */
    public static ProcessorSupplier supplier(String watchedDirectory, String charset) {
        return new Supplier(watchedDirectory, charset);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;
        private final String watchedDirectory;
        private final String charset;

        private transient ArrayList<StreamFilesP> readers;

        Supplier(String watchedDirectory, String charset) {
            this.watchedDirectory = watchedDirectory;
            this.charset = charset;
        }

        @Override @Nonnull
        public List<StreamFilesP> get(int count) {
            readers = new ArrayList<>(count);
            Charset charsetObj = charset == null ? StandardCharsets.UTF_8 : Charset.forName(charset);
            for (int i = 0; i < count; i++) {
                readers.add(new StreamFilesP(watchedDirectory, charsetObj, count, i));
            }
            return readers;
        }

        @Override
        public void complete(Throwable error) {
            readers.forEach(r -> uncheckRun(r::close));
        }
    }

}
