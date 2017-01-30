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
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
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

/**
 * A source that reads files from directory and emits them lines by line.
 *
 * This source is mainly aimed for testing a simple streaming setup that reads from files.
 *
 * <p>
 * {@link WatchType} parameter controls how new and modifies files are
 * handled by the {@code Processor}.
 */
public class FileStreamReader extends AbstractProcessor implements Closeable {

    private final WatchType watchType;
    private final int parallelism;
    private final int id;
    private final Path path;

    private final Map<String, Long> fileOffsets = new HashMap<>();

    private WatchService watcher;

    public enum WatchType {

        /**
         * Process only new files. Modified files will not be re-read.
         */
        NEW,

        /**
         * Re-read files from beginning when a modification occurs on them. The whole
         * fill will be emitted every time there is a modification.
         */
        REPROCESS,

        /**
         * Read only appended content to the files. Only the new content since the
         * last read will be emitted.
         */
        APPENDED_ONLY
    }


    FileStreamReader(String folder, WatchType watchType, int parallelism, int id) {
        this.watchType = watchType;
        this.parallelism = parallelism;
        this.id = id;
        this.path = Paths.get(folder);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        watcher = FileSystems.getDefault().newWatchService();
        path.register(watcher, new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE}, HIGH);
        getLogger().info("Started to watch the directory: " + path);
    }

    @Override
    public boolean complete() {
        try {
            return tryComplete();
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
            final Path filePath = path.resolve(((WatchEvent<Path>) event).context());

            if (kind == ENTRY_DELETE && filePath.equals(path)) {
                getLogger().info("Directory " + path + " deleted, stopped watching");
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
        if (kind == ENTRY_DELETE) {
            getLogger().info("Deteceted deleted file: " + file);
            fileOffsets.remove(file.toString());
        } else if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
            getLogger().info("Detected modified file: " + file);
            long previousOffset = watchType == WatchType.REPROCESS ?
                    0L : fileOffsets.computeIfAbsent(file.toString(), s -> 0L);

            long newOffset = processFile(file, previousOffset);

            if (watchType == WatchType.APPENDED_ONLY) {
                fileOffsets.put(file.toString(), newOffset);
            }
        }
    }

    private boolean shouldProcessEvent(Path file) {
        int hashCode = file.toFile().getPath().hashCode();
        return ((hashCode & Integer.MAX_VALUE) % parallelism) == id;
    }

    private long processFile(Path file, long offset) throws IOException {
        try (FileInputStream fis = new FileInputStream(file.toFile())) {
            fis.getChannel().position(offset);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"))) {
                for (String line; (line = reader.readLine()) != null; ) {
                    getLogger().finest("line = " + line);
                    emit(line);
                }
                return fis.getChannel().position();
            }
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
     * Creates a supplier for {@link FileStreamReader}
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

        private transient ArrayList<FileStreamReader> readers;

        Supplier(String folderPath, WatchType watchType) {
            this.folderPath = folderPath;
            this.watchType = watchType;
        }

        @Override @Nonnull
        public List<FileStreamReader> get(int count) {
            readers = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                readers.add(new FileStreamReader(folderPath, watchType, count, i));
            }
            return readers;
        }

        @Override
        public void complete(Throwable error) {
            readers.forEach(r -> uncheckRun(r::close));
        }
    }

}
