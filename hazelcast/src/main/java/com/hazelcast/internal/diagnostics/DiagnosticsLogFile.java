/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import static com.hazelcast.internal.diagnostics.Diagnostics.MAX_ROLLED_FILE_COUNT;
import static com.hazelcast.internal.diagnostics.Diagnostics.MAX_ROLLED_FILE_SIZE_MB;
import static com.hazelcast.nio.IOUtil.closeResource;
import static java.lang.Math.round;
import static java.lang.String.format;

/**
 * Represents the PerformanceLogFile.
 *
 * Should only be called from the {@link Diagnostics}.
 */
final class DiagnosticsLogFile {

    private static final int ONE_MB = 1024 * 1024;
    private static final int INITIAL_CHAR_BUFF_SIZE = 4 * 1024;

    // points to the file where the log content is written to
    volatile File file;

    private final Diagnostics diagnostics;
    private final ILogger logger;
    private final String fileName;

    private char[] charBuff = new char[INITIAL_CHAR_BUFF_SIZE];
    private int index;
    private BufferedWriter bufferedWriter;
    private int maxRollingFileCount;
    private int maxRollingFileSizeBytes;
    private final DiagnosticsLogWriter logWriter;
    // calling File.length generates a lot of litter; so we'll track it ourselves
    private long fileLength;

    DiagnosticsLogFile(Diagnostics diagnostics) {
        this.diagnostics = diagnostics;
        this.logWriter = diagnostics.singleLine
                ? new SingleLineDiagnosticsLogWriter()
                : new MultiLineDiagnosticsLogWriter();
        this.logger = diagnostics.logger;
        this.fileName = diagnostics.fileName + "-%03d.log";

        this.maxRollingFileCount = diagnostics.properties.getInteger(MAX_ROLLED_FILE_COUNT);
        // we accept a float so it becomes easier to testing to create a small file
        this.maxRollingFileSizeBytes = round(
                ONE_MB * diagnostics.properties.getFloat(MAX_ROLLED_FILE_SIZE_MB));

        logger.finest("maxRollingFileSizeBytes:" + maxRollingFileSizeBytes + " maxRollingFileCount:" + maxRollingFileCount);
    }

    public void write(DiagnosticsPlugin plugin) {
        try {
            if (file == null) {
                file = new File(diagnostics.directory, format(fileName, index));
                bufferedWriter = newWriter();
                renderStaticPlugins();
            }

            renderPlugin(plugin);

            bufferedWriter.flush();
            if (fileLength >= maxRollingFileSizeBytes) {
                rollover();
            }
        } catch (IOException e) {
            logger.warning("Failed to write to file:" + file.getAbsolutePath(), e);
            file = null;
            closeResource(bufferedWriter);
            bufferedWriter = null;
        } catch (RuntimeException e) {
            logger.warning("Failed to write file: " + file, e);
        }
    }

    private void renderStaticPlugins() throws IOException {
        for (DiagnosticsPlugin plugin : diagnostics.staticTasks.get()) {
            renderPlugin(plugin);
        }
    }

    private void renderPlugin(DiagnosticsPlugin plugin) throws IOException {
        logWriter.clean();
        plugin.run(logWriter);
        //bufferedWriter.append()
        int desiredLength = charBuff.length;
        int actualSize = logWriter.length();
        while (desiredLength < actualSize) {
            desiredLength *= 2;
        }

        if (desiredLength != charBuff.length) {
            charBuff = new char[desiredLength];
        }

        logWriter.copyInto(charBuff);
        bufferedWriter.write(charBuff, 0, actualSize);
        fileLength += actualSize;
    }

    private BufferedWriter newWriter() throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(file, true);
        CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
        return new BufferedWriter(new OutputStreamWriter(fos, encoder));
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void rollover() {
        closeResource(bufferedWriter);
        bufferedWriter = null;
        file = null;
        fileLength = 0;
        index++;

        File file = new File(format(fileName, index - maxRollingFileCount));
        // we don't care if the file was deleted or not
        file.delete();
    }
}
