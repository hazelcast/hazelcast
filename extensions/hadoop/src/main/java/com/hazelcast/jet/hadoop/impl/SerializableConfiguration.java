/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This class is used to make {@link Configuration} object serializable.
 */
public final class SerializableConfiguration extends Configuration implements Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused") // for deserialization
    SerializableConfiguration() {
    }

    private SerializableConfiguration(Configuration jobConf) {
        super(jobConf);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        super.write(new DataOutputStream(out));
    }

    private void readObject(ObjectInputStream in) throws IOException {
        super.readFields(new DataInputStream(in));
    }

    public static Configuration asSerializable(Configuration conf) {
        // prevent double wrapping
        if (conf instanceof Serializable) {
            return conf;
        }
        if (conf instanceof JobConf) {
            return new SerializableJobConf((JobConf) conf);
        } else {
            return new SerializableConfiguration(conf);
        }
    }
}
