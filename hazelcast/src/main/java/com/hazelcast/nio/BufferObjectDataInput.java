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
public interface BufferObjectDataInput extends ObjectDataInput, Closeable {

    int read(int index) throws IOException;

    int read(int index, byte b[], int off, int len) throws IOException;

    int readInt(int index) throws IOException;

    long readLong(int index) throws IOException;

    boolean readBoolean(int index) throws IOException;

    byte readByte(int index) throws IOException;

    char readChar(int index) throws IOException;

    double readDouble(int index) throws IOException;

    float readFloat(int index) throws IOException;

    short readShort(int index) throws IOException;

    int position();

    void position(int newPos);

    void reset();

    BufferObjectDataInput duplicate();

    BufferObjectDataInput slice();

}
