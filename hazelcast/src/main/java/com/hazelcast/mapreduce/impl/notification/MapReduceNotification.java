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

package com.hazelcast.mapreduce.impl.notification;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Base class for all map reduce framework notifications
 */
public abstract class MapReduceNotification
        implements IdentifiedDataSerializable {

    private String name;
    private String jobId;

    public MapReduceNotification() {
    }

    public MapReduceNotification(String name, String jobId) {
        this.name = name;
        this.jobId = jobId;
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        jobId = in.readUTF();
    }

    @Override
    public String toString() {
        return "MapReduceNotification{" + "name='" + name + '\'' + ", jobId='" + jobId + '\'' + '}';
    }

}
