/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static java.lang.Math.round;
import static java.lang.String.format;

/**
 * Represents the PerformanceLogFile.
 * <p>
 * Should only be called from the {@link Diagnostics}.
 */
final class DiagnosticsLogFile implements DiagnosticsLog {

    private static final int ONE_MB = 1024 * 1024;

    // points to the file where the log content is written to
    volatile File file;

    private final Diagnostics diagnostics;
    private final ILogger logger;
    private final String fileName;
    private final DiagnosticsLogWriterImpl logWriter;

    private int index;
    private PrintWriter printWriter;
    private final int maxRollingFileCount;
    private final int maxRollingFileSizeBytes;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private Object lock = new Object();

    DiagnosticsLogFile(Diagnostics diagnostics) {
        this.diagnostics = diagnostics;
        this.logger = diagnostics.logger;
        this.fileName = diagnostics.getFileName() + "-%03d.log";
        this.logWriter = new DiagnosticsLogWriterImpl(diagnostics.isIncludeEpochTime(), diagnostics.logger);

        createDirectoryIfDoesNotExist();

        this.maxRollingFileCount = diagnostics.getMaxRollingFileCount();
        // we accept a float, so it becomes easier to testing to create a small file
        this.maxRollingFileSizeBytes = round(
                ONE_MB * diagnostics.getMaxRollingFileSizeMB());

        logger.finest("maxRollingFileSizeBytes:%s maxRollingFileCount:%s", maxRollingFileSizeBytes, maxRollingFileCount);
    }

    @Override
    public void write(DiagnosticsPlugin plugin) {
        synchronized (lock) {
            if (!closed.compareAndSet(false, false)) {
                return;
            }
            try {
                if (file == null) {
                    file = newFile(index, false);
                    printWriter = newWriter();
                    renderStaticPlugins();
                }

                renderPlugin(plugin);

                printWriter.flush();
                if (file.length() >= maxRollingFileSizeBytes) {
                    rollover();
                }
            } catch (IOException e) {
                logger.warning("Failed to write to file:" + file.getAbsolutePath(), e);
                close();
            } catch (RuntimeException e) {
                logger.warning("Failed to write file: " + file, e);
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            closeResource(printWriter);
        }
    }

    // just for testing
    int getMaxRollingFileCount() {
        return maxRollingFileCount;
    }

    // just for testing
    int getMaxRollingFileSizeBytes() {
        return maxRollingFileSizeBytes;
    }

    private File newFile(int index, boolean silent) {
        createDirectoryIfDoesNotExist();
        if (!silent && index == 0) {
            logger.info("Diagnostics log directory is [" + diagnostics.getLoggingDirectory() + "]");
        }
        return new File(diagnostics.getLoggingDirectory(), format(fileName, index));
    }

    private void createDirectoryIfDoesNotExist() {
        File dir = diagnostics.getLoggingDirectory();
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new InvalidConfigurationException("Configured path for diagnostics log file '" + dir
                        + "' exists, but it's not a directory");
            }

            if (!canWriteToDirectory(dir)) {
                throw new InvalidConfigurationException("Cannot write to diagnostics log directory '" + dir
                        + "'. Check filesystem permissions.");
            }
        } else {
            if (!dir.mkdirs()) {
                throw new InvalidConfigurationException("Error while creating a directory '" + dir
                        + "' for diagnostics log files. Check filesystem permissions.");
            }
        }
    }

    private void renderStaticPlugins() {
        for (DiagnosticsPlugin plugin : diagnostics.staticTasks.get()) {
            renderPlugin(plugin);
        }
    }

    private void renderPlugin(DiagnosticsPlugin plugin) {
        logWriter.resetSectionLevel();
        logWriter.init(printWriter);
        plugin.run(logWriter);
    }

    private PrintWriter newWriter() throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(file, true);
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
        return new PrintWriter(new BufferedWriter(new OutputStreamWriter(fos, encoder), Short.MAX_VALUE));
    }

    private void rollover() {
        closeResource(printWriter);
        printWriter = null;
        file = null;

        File file = newFile(index - maxRollingFileCount, true);
        deleteQuietly(file);
        index++;
    }

    private boolean canWriteToDirectory(File dir) {
        if (!dir.isDirectory() || !dir.canWrite()) {
            return false;
        }
        File tempFile = null;
        try {
            tempFile = File.createTempFile("writetest", ".tmp", dir);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
        }
    }
}
