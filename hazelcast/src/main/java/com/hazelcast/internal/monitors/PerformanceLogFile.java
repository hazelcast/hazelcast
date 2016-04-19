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

package com.hazelcast.internal.monitors;

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
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.monitors.PerformanceMonitor.DIRECTORY;
import static com.hazelcast.internal.monitors.PerformanceMonitor.MAX_ROLLED_FILE_COUNT;
import static com.hazelcast.internal.monitors.PerformanceMonitor.MAX_ROLLED_FILE_SIZE_MB;
import static com.hazelcast.nio.IOUtil.closeResource;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.lang.System.arraycopy;

/**
 * A {@link PerformanceLog} that writes to file.
 */
final class PerformanceLogFile extends PerformanceLog {

    private static final int ONE_MB = 1024 * 1024;
    private static final int INITIAL_CHAR_BUFF_SIZE = 4 * 1024;

    final AtomicReference<PerformanceMonitorPlugin[]> staticPlugins = new AtomicReference<PerformanceMonitorPlugin[]>(
            new PerformanceMonitorPlugin[0]
    );

     // points to the file where the log content is written to
    volatile File file;

    private final ILogger logger;
    private final String fileName;
    private final String directory;
    private char[] charBuff = new char[INITIAL_CHAR_BUFF_SIZE];
    private int index;
    private BufferedWriter bufferedWriter;
    private int maxRollingFileCount;
    private int maxRollingFileSizeBytes;
    // calling File.length generates a lot of litter; so we'll track it ourselves
    private long fileLength;

    PerformanceLogFile(PerformanceMonitor performanceMonitor) {
        super(performanceMonitor);
        this.logger = performanceMonitor.logger;
        this.fileName = performanceMonitor.fileName + "-%03d.log";
        this.directory = performanceMonitor.properties.getString(DIRECTORY);

        this.maxRollingFileCount = performanceMonitor.properties.getInteger(MAX_ROLLED_FILE_COUNT);
        // we accept a float so it becomes easier to testing to create a small file
        this.maxRollingFileSizeBytes = round(
                ONE_MB * performanceMonitor.properties.getFloat(MAX_ROLLED_FILE_SIZE_MB));

        logger.finest("maxRollingFileSizeBytes:" + maxRollingFileSizeBytes + " maxRollingFileCount:" + maxRollingFileCount);
    }

    @Override
    public void addStaticPlugin(PerformanceMonitorPlugin plugin) {
        for (; ; ) {
            PerformanceMonitorPlugin[] oldPlugins = staticPlugins.get();
            PerformanceMonitorPlugin[] newPlugins = new PerformanceMonitorPlugin[oldPlugins.length + 1];
            arraycopy(oldPlugins, 0, newPlugins, 0, oldPlugins.length);
            newPlugins[oldPlugins.length] = plugin;
            if (staticPlugins.compareAndSet(oldPlugins, newPlugins)) {
                break;
            }
        }
    }

    @Override
    public void render(PerformanceMonitorPlugin plugin) {
        try {
            if (file == null) {
                file = new File(directory, format(fileName, index));
                bufferedWriter = newWriter();
                for (PerformanceMonitorPlugin staticPlugin : staticPlugins.get()) {
                    render(staticPlugin);
                }
            }

            render0(plugin);

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

    private void render0(PerformanceMonitorPlugin plugin) throws IOException {
        logWriter.write(plugin);
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
