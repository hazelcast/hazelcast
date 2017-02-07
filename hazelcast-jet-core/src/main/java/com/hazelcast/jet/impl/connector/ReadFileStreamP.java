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
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
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

/**
 * A source that reads files from a directory and emits them line by line. It processes files,
 * as they are created/modified. It completes, when the directory is deleted.
 *
 * <p>This source is mainly aimed for testing a simple streaming setup that reads from files.
 *
 * <p>
 * {@link WatchType} parameter controls how new and modified files are
 * handled by the {@code Processor}.
 */
public class ReadFileStreamP extends AbstractProcessor implements Closeable {

    private final WatchType watchType;
    private final int parallelism;
    private final int id;
    private final Path watchedDirectory;

    private final Map<Path, Long> fileOffsets;

    private WatchService watcher;

    public enum WatchType {

        /**
         * Process only new files. Modified or pre-existing files will not be re-read.
         *
         * <p><b>Warning: </b>Might not work reliably, as the OS may report
         * new file before its contents are created, just after creation. This might
         * change by operating system, file system type, encryption/compression,
         * network drives, etc.
         */
        NEW,

        /**
         * Re-read files from the beginning when a modification occurs. The whole
         * file will be emitted every time there is a modification. Pre-existing files will
         * be emitted upon first modification.
         *
         * <p><b>Warning: </b>A file being written to might be picked up multiple
         * times during that period, with possibly incomplete lines.
         */
        REPROCESS,

        /**
         * Read only appended content to the files. Only the new content since the
         * last read will be emitted. Pre-existing files will be processed entirely upon
         * first modification.
         *
         * <p><b>Warning: </b>A file being written to might be picked up at any
         * moment during that period, possibly splitting the lines or even words where
         * they are not split.
         */
        APPENDED_ONLY
    }


    ReadFileStreamP(String folder, WatchType watchType, int parallelism, int id) {
        this.watchType = watchType;
        this.parallelism = parallelism;
        this.id = id;
        this.watchedDirectory = Paths.get(folder);

        if (watchType == WatchType.APPENDED_ONLY) {
            fileOffsets = new HashMap<>();
        } else {
            fileOffsets = null;
        }
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        watcher = FileSystems.getDefault().newWatchService();
        watchedDirectory.register(watcher, new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE}, HIGH);
        getLogger().info("Started to watch the directory: " + watchedDirectory);
    }

    @Override
    public boolean complete() {
        try {
            boolean isDone = tryComplete();
            if (isDone) {
                close();
            }
            return isDone;
        } catch (InterruptedException ignored) {
            return true;
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    private boolean tryComplete() throws InterruptedException, IOException {
        WatchKey key = watcher.take();
        for (WatchEvent<?> event : key.pollEvents()) {
            final WatchEvent.Kind<?> kind = event.kind();
            final Path filePath = watchedDirectory.resolve(((WatchEvent<Path>) event).context());

            if (kind == OVERFLOW) {
                getLogger().warning("Detected OVERFLOW in " + watchedDirectory);
                continue;
            }

            if (kind == ENTRY_DELETE && filePath.equals(watchedDirectory)) {
                getLogger().info("Directory " + watchedDirectory + " deleted, stopped watching");
                return true;
            }

            if (shouldProcessEvent(filePath)) {
                processEvent(kind, filePath);
            }
        }
        if (!key.reset()) {
            getLogger().info("Watch key is invalid. Stopping watcher.");
            return true;
        }
        return false;
    }

    private void processEvent(Kind<?> kind, Path file) throws IOException {
        // ignore subdirectories
        if (Files.isDirectory(file)) {
            return;
        }

        if (kind == ENTRY_DELETE && watchType == WatchType.APPENDED_ONLY) {
            if (getLogger().isFineEnabled()) {
                getLogger().fine("Detected deleted file: " + file);
            }
            if (fileOffsets != null) {
                fileOffsets.remove(file);
            }
        } else if (kind == ENTRY_CREATE) {
            if (getLogger().isFineEnabled()) {
                getLogger().fine("Detected new file: " + file);
            }
            long newOffset = processFile(file, 0L);
            if (fileOffsets != null) {
                fileOffsets.put(file, newOffset);
            }
        } else if (kind == ENTRY_MODIFY && watchType != WatchType.NEW) {
            if (getLogger().isFineEnabled()) {
                getLogger().fine("Detected modified file: " + file);
            }

            long previousOffset = fileOffsets != null ? fileOffsets.getOrDefault(file, 0L) : 0L;
            long newOffset = processFile(file, previousOffset);

            if (fileOffsets != null) {
                fileOffsets.put(file, newOffset);
            }
        }
    }

    private boolean shouldProcessEvent(Path file) {
        int hashCode = file.hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == id;
    }

    private long processFile(Path file, long offset) throws IOException {
        try (FileInputStream fis = new FileInputStream(file.toFile())) {
            fis.getChannel().position(offset);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8))) {
                for (String line; (line = reader.readLine()) != null; ) {
                    if (getLogger().isFinestEnabled()) {
                        getLogger().finest("line = " + line);
                    }
                    emit(line);
                }
                return fis.getChannel().position();
            }
        } catch (FileNotFoundException ignored) {
            // ignore missing file: on file deletion, ENTRY_MODIFY is reported just before ENTRY_DELETE
            return offset;
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (watcher != null) {
            getLogger().info("Closing watcher");
            watcher.close();
        }
    }

    /**
     * Creates a supplier for {@link ReadFileStreamP}
     *
     * @param folderPath the folder to watch
     * @param watchType  the
     */
    public static ProcessorSupplier supplier(String folderPath, WatchType watchType) {
        return new Supplier(folderPath, watchType);
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;
        private final String folderPath;
        private final WatchType watchType;

        private transient ArrayList<ReadFileStreamP> readers;

        Supplier(String folderPath, WatchType watchType) {
            this.folderPath = folderPath;
            this.watchType = watchType;
        }

        @Override @Nonnull
        public List<ReadFileStreamP> get(int count) {
            readers = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                readers.add(new ReadFileStreamP(folderPath, watchType, count, i));
            }
            return readers;
        }

        @Override
        public void complete(Throwable error) {
            readers.forEach(r -> uncheckRun(r::close));
        }
    }

}
