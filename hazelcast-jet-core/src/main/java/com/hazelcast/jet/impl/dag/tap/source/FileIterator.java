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

package com.hazelcast.jet.impl.dag.tap.source;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.io.tuple.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.util.Iterator;

public class FileIterator implements Iterator<Tuple<Integer, String>> {
    private String line;
    private LineNumberReader raf;
    private boolean hasNext = true;
    private ByteCountingInputStream byteCountingStream;
    private int lineNumber;

    public FileIterator(File file,
                        long start,
                        long end) {
        try {
            this.byteCountingStream = new ByteCountingInputStream(new FileInputStream(file), end);
            this.raf = new LineNumberReader(new BufferedReader(
                    new InputStreamReader(byteCountingStream, Charset.forName("UTF-8"))
            ));

            if (start > 0) {
                if (byteCountingStream.skip(start) < 0) {
                    throw new JetException("Can't read from file inputStream");
                }
            }
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    private void close() throws IOException {
        hasNext = false;

        if (byteCountingStream != null) {
            byteCountingStream.close();
            byteCountingStream = null;
        }

        if (raf != null) {
            raf.close();
            raf = null;
        }
    }

    public long getLineNumber() {
        return byteCountingStream.getLineNumber();
    }

    @Override
    public boolean hasNext() {
        try {
            if (!hasNext) {
                return false;
            }

            if (line == null) {
                line = raf.readLine();
                lineNumber = raf.getLineNumber();

                if (line == null) {
                    close();
                    return false;
                }
            }

            return true;
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public Tuple<Integer, String> next() {
        if (!hasNext()) {
            throw new IllegalStateException("Iterator closed");
        }

        if (line == null) {
            return null;
        }

        try {
            return new JetTuple2<>(lineNumber, line);
        } finally {
            line = null;
        }
    }
}
