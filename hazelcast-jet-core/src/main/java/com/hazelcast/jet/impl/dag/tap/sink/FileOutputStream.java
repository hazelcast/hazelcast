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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.dag.tap.SinkOutputStream;
import com.hazelcast.jet.dag.tap.SinkTap;
import com.hazelcast.jet.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.impl.util.JetUtil;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

public class FileOutputStream implements SinkOutputStream {
    private static final long serialVersionUID = -396575576353368113L;

    private final SinkTap tap;
    private final String name;

    private boolean closed = true;
    private transient Writer fileWriter;

    public FileOutputStream(String name, SinkTap tap) {
        this.tap = tap;
        this.name = name;
    }

    @Override
    public void open() {
        if (this.closed) {
            if (this.tap.getTapStrategy() == SinkTapWriteStrategy.CLEAR_AND_REPLACE) {
                File file = new File(this.name);

                if (file.exists()) {
                    initFile(file);
                }
            }

            try {
                this.fileWriter = new OutputStreamWriter(
                        new java.io.FileOutputStream(new File(this.name)),
                        Charset.forName("UTF-8")
                );
            } catch (IOException e) {
                throw JetUtil.reThrow(e);
            }

            this.closed = false;
        }
    }

    private void initFile(File file) {
        if (!file.setWritable(true)) {
            throw new JetException("Can't write to file " + file.getName());
        }

        if (!file.delete()) {
            throw new JetException("Can't delete file " + file.getName());
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
