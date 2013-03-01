/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import java.io.Closeable;
import java.io.IOException;

/**
 * @mdogan 12/28/12
 */
public interface BufferObjectDataOutput extends ObjectDataOutput, Closeable {

    void write(int index, int b);

    void write(int index, byte b[], int off, int len);

    void writeInt(int index, int v) throws IOException;

    void writeLong(int index, final long v) throws IOException;

    void writeBoolean(int index, final boolean v) throws IOException;

    void writeByte(int index, final int v) throws IOException;

    void writeChar(int index, final int v) throws IOException;

    void writeDouble(int index, final double v) throws IOException;

    void writeFloat(int index, final float v) throws IOException;

    void writeShort(int index, final int v) throws IOException;

    int position();

    void position(int newPos);

    byte[] getBuffer();

    void reset();

    BufferObjectDataOutput duplicate();

    BufferObjectDataOutput slice();

    byte[] toByteArray();

}
