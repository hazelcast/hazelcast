/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.tpc.engine;

import net.smacke.jaydio.DirectIoLib;

/**
 * Represents a File that can only be accessed asynchronously.
 *
 * This class isn't thread-safe. It should only be processed by the owning {@link Eventloop}.
 */
public abstract class AsyncFile {

    private static final int pageSize = DirectIoLib.getpagesize();

    public static final int O_RDONLY = 0;
    public static final int O_WRONLY = 1;
    public static final int O_RDWR = 2;
    public static final int O_CREAT = 64;
    public static final int O_TRUNC = 512;
    public static final int O_DIRECT = 16384;
    public static final int O_SYNC = 1048576;

    /**
     * Returns the file descriptor.
     *
     * If {@link #open(int)} hasn't been called, then the value is undefined.
     *
     * @return the file decriptor.
     */
    public abstract int fd();

    public abstract Fut nop();

    /**
     * Returns the path to the file.
     *
     * @return the path to the file.
     */
    public abstract String path();

    /**
     * Returns the page size of the OS.
     *
     * @return the page size.
     */
    public static int pageSize() {
        return pageSize;
    }

    public abstract Fut fsync();

    public abstract Fut fallocate(int mode, long offset, long len);

    public abstract Fut delete();

    public abstract Fut open(int flags);

    public abstract Fut close();

    /**
     * Reads data from a file from some offset.
     *
     * todo:
     * The problem is that we can't just pass any buffer. In case of DIO, the buffer
     * needs to be properly aligned. So if there would be a map.get, you can't just start adding
     * the data after.
     *
     * todo: use an IOBuffer instead of an bufferAddress/length. This way can integrate with
     * AsyncFileChannel.
     *
     * @param offset the offset.
     * @param length the number of bytes to read.
     * @return
     */
    public abstract Fut pread(long offset, int length, long bufferAddress);

    /**
     * todo: use an IOBuffer instead of an bufferAddress/length. This way can integrate with
     * AsyncFileChannel.
     *
     * @param offset
     * @param length
     * @param bufferAddress
     * @return
     */
    public abstract Fut pwrite(long offset, int length, long bufferAddress);
}
