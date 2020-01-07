/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio;

import com.hazelcast.internal.serialization.Data;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Extends {@link DataOutput} with the ability to write {@link Data} to the output stream.
 */
public interface DataWriter extends DataOutput {
    /**
     * @param data data to be written
     * @throws IOException
     */
    void writeData(Data data) throws IOException;
}
