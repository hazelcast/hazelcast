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

package com.hazelcast.internal.tpc;

/**
 * Represents a File that can only be accessed asynchronously.
 * <p/>
 * This class isn't thread-safe. It should only be processed by the owning {@link Eventloop}.
 */
public abstract class AsyncFile {

    public static final int PERMISSIONS_ALL = 0666;

    public static final int O_RDONLY = 00;
    public static final int O_WRONLY = 01;
    public static final int O_RDWR = 02;
    public static final int O_CREAT = 0100;
    public static final int O_TRUNC = 01000;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04000000;

    /**
     * Returns the file descriptor.
     * <p/>
     * If {@link #open(int)} hasn't been called, then the value is undefined.
     *
     * @return the file decriptor.
     */
    public abstract int fd();

    /**
     * Executes a noop asynchronously. This method exists purely for benchmarking
     * purposes and is made for the io_uring NOOP.
     * </p>
     * Any other implementation is free to ignore it.
     *
     * @return a future.
     */
    public abstract Promise nop();

    /**
     * Returns the path to the file.
     *
     * @return the path to the file.
     */
    public abstract String path();

    public abstract Promise<Integer> fsync();

    public abstract Promise<Integer> fdatasync();

    public abstract Promise<Integer> fallocate(int mode, long offset, long len);

    public abstract Promise<Integer> delete();

    public abstract Promise<Integer> open(int flags, int permissions);

    public abstract Promise<Integer> close();

    /**
     * Reads data from a file from some offset.
     * <p>
     * todo:
     * The problem is that we can't just pass any buffer. In case of DIO, the buffer
     * needs to be properly aligned. So if there would be a map.get, you can't just start adding
     * the data after.
     * <p>
     * todo: use an IOBuffer instead of an bufferAddress/length. This way can integrate with
     * AsyncFileChannel.
     *
     * @param offset the offset.
     * @param length the number of bytes to read.
     * @return a Fut with the response code of the request.
     */
    public abstract Promise<Integer> pread(long offset, int length, long bufferAddress);

    /**
     * Writes data to a file from the given offset.
     * <p>
     * todo: use an IOBuffer instead of an bufferAddress/length. This way can integrate with
     * AsyncFileChannel.
     *
     * @param offset
     * @param length
     * @param bufferAddress
     * @return a Fut with the response code of the request.
     */
    public abstract Promise<Integer> pwrite(long offset, int length, long bufferAddress);

    public abstract Promise<Integer> pwrite(long offset, int length, long bufferAddress, int flags, int rwFlags);
}
