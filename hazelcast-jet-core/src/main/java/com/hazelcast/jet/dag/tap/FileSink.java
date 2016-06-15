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

package com.hazelcast.jet.dag.tap;

import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.impl.dag.tap.sink.DataFileWriter;
import com.hazelcast.jet.impl.dag.tap.sink.FileOutputStream;
import com.hazelcast.spi.NodeEngine;

public class FileSink implements SinkTap {

    private final String filename;
    private final FileOutputStream fileOutputStream;

    public FileSink(String filename) {
        this.filename = filename;
        this.fileOutputStream = new FileOutputStream(filename);
    }

    @Override
    public DataWriter[] getWriters(NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
        return new DataWriter[] {
                new DataFileWriter(containerDescriptor, 0, fileOutputStream),
        };
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }

    @Override
    public String getName() {
        return filename;
    }
}
