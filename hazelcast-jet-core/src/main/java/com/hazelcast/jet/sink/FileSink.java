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

package com.hazelcast.jet.sink;

import com.hazelcast.jet.Sink;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.impl.dag.sink.DataFileWriter;
import com.hazelcast.jet.impl.dag.sink.FileOutputStream;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.runtime.DataWriter;

/**
 * A sink which uses a file as output.
 */
public class FileSink implements Sink {

    private final String filename;
    private final FileOutputStream fileOutputStream;

    /**
     * Constructs a sink with the given filename
     *
     * @param filename the output filename
     */
    public FileSink(String filename) {
        this.filename = filename;
        this.fileOutputStream = new FileOutputStream(filename);
    }

    @Override
    public DataWriter[] getConsumers(JobContext jobContext, Vertex vertex) {
        return new DataWriter[]{new DataFileWriter(jobContext, 0, fileOutputStream)};
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }

    @Override
    public String getName() {
        return filename;
    }

    @Override
    public String toString() {
        return "FileSink{"
                + "name='" + filename + '\''
                + '}';
    }
}
