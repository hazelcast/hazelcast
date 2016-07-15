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

package com.hazelcast.jet.memory.spilling;

import com.hazelcast.jet.memory.JetMemoryException;
import com.hazelcast.jet.memory.util.Util;
import com.hazelcast.nio.IOUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Base class for spiller implementations.
 */
abstract class SpillerBase {
    protected File activeFile;

    protected File tempFile;

    protected final boolean useBigEndian;

    protected final SpillFileCursor spillFileCursor;

    protected final SpillingKeyValueWriter recordWriter;

    protected final SpillFileReader input;

    protected final SpillFileWriter output;

    private final File baseDir;

    protected SpillerBase(File baseDir, int bufferSize, boolean useBigEndian) {
        this.baseDir = baseDir;
        this.useBigEndian = useBigEndian;
        this.spillFileCursor = new SpillFileCursor();
        this.recordWriter = new SpillingKeyValueWriter();
        this.input = new SpillFileReader(bufferSize, useBigEndian);
        this.output = new SpillFileWriter(bufferSize, useBigEndian);
    }

    public SpillFileCursor openSpillFileCursor() {
        if (activeFile != null) {
            try {
                input.setInput(new FileInputStream(activeFile));
            } catch (IOException e) {
                input.close();
            }
            spillFileCursor.open(input);
        }
        return spillFileCursor;
    }

    public void stop() {
        try {
            output.flush();
        } catch (IOException e) {
            throw Util.rethrow(e);
        } finally {
            input.close();
            output.close();
            spillFileCursor.close();
            recordWriter.close();
        }
        delete(activeFile);
        if (!tempFile.renameTo(activeFile)) {
            throw new JetMemoryException("Can't rename temporary file "
                    + tempFile.getAbsolutePath() + " to file with name " + activeFile.getAbsolutePath()
            );
        }
        tempFile = null;
    }

    public void dispose() {
        delete(activeFile);
        delete(tempFile);
    }

    private static void delete(File f) {
        if (f != null) {
            IOUtil.delete(f);
        }
    }

    protected final void ensureSpillFiles() {
        try {
            if (activeFile == null) {
                activeFile = File.createTempFile("jet", "spilling", baseDir);
            }
            if (tempFile == null) {
                tempFile = File.createTempFile("jet", "spilling-temporary", baseDir);
            }
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }
}
