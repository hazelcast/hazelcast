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

import com.hazelcast.internal.nio.ClassLoaderUtil;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * Wrapper of {@code InputSplit} that adds serializability and sortability
 * by the position of the split in the HDFS file.
 */
class IndexedInputSplit implements Comparable<IndexedInputSplit>, Serializable {

    private static final long serialVersionUID = 1L;

    private int index;
    private org.apache.hadoop.mapred.InputSplit oldSplit;
    private org.apache.hadoop.mapreduce.InputSplit newSplit;
    private boolean isOld;

    IndexedInputSplit(int index, org.apache.hadoop.mapred.InputSplit split) {
        this.index = index;
        this.oldSplit = split;
        this.isOld = true;
    }

    IndexedInputSplit(int index, org.apache.hadoop.mapreduce.InputSplit split) {
        this.index = index;
        this.newSplit = split;
        this.isOld = false;
    }

    @Nullable
    org.apache.hadoop.mapred.InputSplit getOldSplit() {
        return oldSplit;
    }

    @Nullable
    org.apache.hadoop.mapreduce.InputSplit getNewSplit() {
        return newSplit;
    }

    List<String> getLocations() throws Exception {
        if (isOld) {
            return Arrays.asList(oldSplit.getLocations());
        } else {
            return Arrays.asList(newSplit.getLocations());
        }
    }

    @Override
    public String toString() {
        try {
            return "IndexedInputSplit{index " + index + ", blocks " + blocksOfSplit()
                    + ", locations " + Arrays.toString(isOld ? oldSplit.getLocations() : newSplit.getLocations()) + '}';
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public int compareTo(@Nonnull IndexedInputSplit other) {
        return Integer.compare(index, other.index);
    }

    @Override
    public boolean equals(Object o) {
        IndexedInputSplit that;
        return this == o ||
                o != null
                        && getClass() == o.getClass()
                        && index == (that = (IndexedInputSplit) o).index
                        && Objects.equals(oldSplit, that.oldSplit)
                        && Objects.equals(newSplit, that.newSplit);
    }

    @Override
    public int hashCode() {
        return 31 * index + Objects.hashCode(oldSplit) + Objects.hashCode(newSplit);
    }

    private void writeObject(ObjectOutputStream out) throws Exception {
        out.writeInt(index);
        out.writeBoolean(isOld);
        if (isOld) {
            out.writeUTF(oldSplit.getClass().getName());
            oldSplit.write(out);
        } else {
            if (!(newSplit instanceof Writable)) {
                throw new IllegalStateException(newSplit.getClass().getName()
                        + " does not implement the org.apache.hadoop.io.Writable interface");
            }
            out.writeUTF(newSplit.getClass().getName());
            ((Writable) newSplit).write(out);
        }
    }

    private void readObject(ObjectInputStream in) throws Exception {
        index = in.readInt();
        isOld = in.readBoolean();
        Object splitClass = ClassLoaderUtil.newInstance(Thread.currentThread().getContextClassLoader(), in.readUTF());
        if (isOld) {
            oldSplit = (org.apache.hadoop.mapred.InputSplit) splitClass;
            oldSplit.readFields(in);
        } else {
            newSplit = (org.apache.hadoop.mapreduce.InputSplit) splitClass;
            ((Writable) newSplit).readFields(in);
        }
    }

    private String blocksOfSplit() {
        final String s = isOld ? oldSplit.toString() : newSplit.toString();
        return s.substring(s.lastIndexOf(':') + 1);
    }
}
