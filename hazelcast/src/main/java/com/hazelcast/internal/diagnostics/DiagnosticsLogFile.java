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

import static com.hazelcast.internal.diagnostics.Diagnostics.MAX_ROLLED_FILE_COUNT;
import static com.hazelcast.internal.diagnostics.Diagnostics.MAX_ROLLED_FILE_SIZE_MB;
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
    private int maxRollingFileCount;
    private int maxRollingFileSizeBytes;

    DiagnosticsLogFile(Diagnostics diagnostics) {
        this.diagnostics = diagnostics;
        this.logger = diagnostics.logger;
        this.fileName = diagnostics.baseFileName + "-%03d.log";
        this.logWriter = new DiagnosticsLogWriterImpl(diagnostics.includeEpochTime, diagnostics.logger);

        this.maxRollingFileCount = diagnostics.properties.getInteger(MAX_ROLLED_FILE_COUNT);
        // we accept a float so it becomes easier to testing to create a small file
        this.maxRollingFileSizeBytes = round(
                ONE_MB * diagnostics.properties.getFloat(MAX_ROLLED_FILE_SIZE_MB));

        logger.finest("maxRollingFileSizeBytes:" + maxRollingFileSizeBytes + " maxRollingFileCount:" + maxRollingFileCount);
    }

    public void write(DiagnosticsPlugin plugin) {
        try {
            if (file == null) {
                file = newFile(index);
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
            file = null;
            closeResource(printWriter);
            printWriter = null;
        } catch (RuntimeException e) {
            logger.warning("Failed to write file: " + file, e);
        }
    }

    private File newFile(int index) {
        createDirectoryIfDoesNotExist();
        logger.info("Diagnostics log directory is [" + diagnostics.directory + "]");
        return new File(diagnostics.directory, format(fileName, index));
    }

    private void createDirectoryIfDoesNotExist() {
        File dir = diagnostics.directory;
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new InvalidConfigurationException("Configured path for diagnostics log file '" + dir
                        + "' exists, but it's not a directory");
            }
        } else {
            if (!dir.mkdirs()) {
                throw new InvalidConfigurationException("Error while creating a directory '" + dir
                        + "' for diagnostics log files. Are you having sufficient rights on the filesystem?");
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
        index++;

        File file = newFile(index - maxRollingFileCount);
        deleteQuietly(file);
    }
}
