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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Adapted from code example from book
 * Hadoop: The Definitive Guide, Fourth Edition by Tom White (O'Reilly, 2014)
 * https://github.com/tomwhite/hadoop-book/blob/master/ch08-mr-types/src/main/java/WholeFileRecordReader.java
 */
abstract class WholeFileRecordReader<T> extends RecordReader<NullWritable, T> {

    private final T value;
    private FileSplit fileSplit;
    private Configuration conf;
    private boolean processed;

    WholeFileRecordReader(T value) {
        this.value = value;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                setValue(contents, 0, contents.length, value);
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }

    protected abstract void setValue(byte[] contents, int start, int length, T value);

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public T getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() {
    }
}
