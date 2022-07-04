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

import org.apache.hadoop.io.BytesWritable;

/**
 * Adapted from code example from book
 * Hadoop: The Definitive Guide, Fourth Edition by Tom White (O'Reilly, 2014)
 * https://github.com/tomwhite/hadoop-book/blob/master/ch08-mr-types/src/main/java/WholeFileRecordReader.java
 */
class WholeFileAsBytesRecordReader extends WholeFileRecordReader<BytesWritable> {

    WholeFileAsBytesRecordReader() {
        super(new BytesWritable());
    }

    @Override
    protected void setValue(byte[] contents, int start, int length, BytesWritable value) {
        value.set(contents, start, length);
    }

}
