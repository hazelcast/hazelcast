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

package com.hazelcast.jet.memory.impl.memory.impl.spilling.spillers;

import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataInput;
import com.hazelcast.jet.memory.api.memory.spilling.io.SpillingDataOutput;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.io.DiskDataInput;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.io.DiskDataOutput;
import com.hazelcast.jet.memory.api.operations.partition.KeyValueDataPartition;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueReader;
import com.hazelcast.jet.memory.api.memory.spilling.format.SpillingKeyValueWriter;
import com.hazelcast.jet.memory.api.memory.spilling.spillers.KeyValueStorageSpiller;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.format.DefaultSpillingKeyValueReader;
import com.hazelcast.jet.memory.impl.memory.impl.spilling.format.DefaultSpillingKeyValueWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;

public abstract class BaseKeyValueSpillerImpl<T, P extends KeyValueDataPartition<T>>
        implements KeyValueStorageSpiller<T, P> {
    protected File activeSpillingFile;

    protected File temporarySpillingFile;

    protected final boolean useBigEndian;

    protected final SpillingKeyValueReader spillingKeyValueReader;

    protected final SpillingKeyValueWriter spillingKeyValueWriter;

    protected final SpillingDataInput<InputStream> spillingDataInput;

    protected final SpillingDataOutput<OutputStream> spillingDataOutput;

    private final File spillingDirectory;

    protected BaseKeyValueSpillerImpl(int spillingBufferSize,
                                      File spillingDirectory,
                                      boolean useBigEndian) {
        this.useBigEndian = useBigEndian;
        this.spillingDirectory = spillingDirectory;
        this.spillingKeyValueReader = new DefaultSpillingKeyValueReader();
        this.spillingKeyValueWriter = new DefaultSpillingKeyValueWriter();
        this.spillingDataInput = new DiskDataInput(spillingBufferSize, useBigEndian);
        this.spillingDataOutput = new DiskDataOutput(spillingBufferSize, useBigEndian);
    }

    protected void checkSpillingFile() {
        try {
            if (activeSpillingFile == null) {
                activeSpillingFile =
                        File.createTempFile("jet", "spilling", spillingDirectory);
            }

            if (temporarySpillingFile == null) {
                temporarySpillingFile =
                        File.createTempFile("jet", "spilling-temporary", spillingDirectory);
            }
        } catch (IOException e) {
            throw Util.reThrow(e);
        }
    }

    @Override
    public SpillingKeyValueReader openSpillingReader() {
        if (activeSpillingFile != null) {
            try {
                this.spillingDataInput.open(new FileInputStream(activeSpillingFile));
            } catch (IOException e) {
                this.spillingDataInput.close();
            }

            spillingKeyValueReader.open(spillingDataInput);
        }

        return spillingKeyValueReader;
    }

    @Override
    public void stop() {
        try {
            spillingDataOutput.flush();
        } catch (IOException e) {
            throw Util.reThrow(e);
        } finally {
            spillingDataInput.close();
            spillingDataOutput.close();
            spillingKeyValueReader.close();
            spillingKeyValueWriter.close();
        }

        Util.deleteFile(activeSpillingFile);

        if (!temporarySpillingFile.renameTo(activeSpillingFile)) {
            throw new IllegalStateException("Can't rename temporary file "
                    +
                    temporarySpillingFile.getAbsolutePath() +
                    " to file with name " + activeSpillingFile.getAbsolutePath()
            );
        }

        temporarySpillingFile = null;
    }

    @Override
    public void dispose() {
        Util.deleteFile(activeSpillingFile);
        Util.deleteFile(temporarySpillingFile);
    }
}
