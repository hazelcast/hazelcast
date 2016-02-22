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

package com.hazelcast.jet.impl.dag.tap.sink;

import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.dag.tap.SinkOutputStream;
import com.hazelcast.jet.spi.dag.tap.SinkTap;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileOutputStream implements SinkOutputStream {
    private final SinkTap tap;
    private final String name;

    private boolean closed = true;
    private transient FileWriter fileWriter;

    public FileOutputStream(String name, SinkTap tap) {
        this.tap = tap;
        this.name = name;
    }

    @Override
    public void open() {
        if (this.closed) {
            System.out.println("Trying fileWriterCreated");

            if (this.tap.getTapStrategy() == SinkTapWriteStrategy.CLEAR_AND_REPLACE) {
                File file = new File(this.name);

                if (file.exists()) {
                    file.setWritable(true);
                    file.delete();
                }
            }

            try {
                this.fileWriter = new FileWriter(new File(this.name));
            } catch (IOException e) {
                throw JetUtil.reThrow(e);
            }

            System.out.println("FileWriterCreated");

            this.closed = false;
        }
    }

    @Override
    public void write(String data) {
        try {
            this.fileWriter.write(data);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public void flush() {
        try {
            this.fileWriter.flush();
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public void close() {
        if (!this.closed) {
            try {
                this.fileWriter.flush();
                this.fileWriter.close();
            } catch (IOException e) {
                throw JetUtil.reThrow(e);
            }

            this.closed = true;
        }
    }
}
