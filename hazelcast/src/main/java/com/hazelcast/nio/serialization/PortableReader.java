/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import java.io.IOException;

/**
 * @mdogan 12/28/12
 */
public interface PortableReader {

    int readInt(String fieldName) throws IOException;

    long readLong(String fieldName) throws IOException;

    String readUTF(String fieldName) throws IOException;

    boolean readBoolean(String fieldName) throws IOException;

    byte readByte(String fieldName) throws IOException;

    char readChar(String fieldName) throws IOException;

    double readDouble(String fieldName) throws IOException;

    float readFloat(String fieldName) throws IOException;

    short readShort(String fieldName) throws IOException;
    
    <P extends Portable> P readPortable(String fieldName) throws IOException;
}
