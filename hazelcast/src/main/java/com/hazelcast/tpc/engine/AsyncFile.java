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

    protected int fd;

    /**
     * Returns the file descriptor.
     *
     * If {@link #open(int)} hasn't been called, then the value is undefined.
     *
     * @return the file decriptor.
     */
    public int fd() {
        return fd;
    }

    public abstract Promise nop();

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

    public abstract Promise fsync();

    public abstract Promise fallocate(int mode, long offset, long len);

    public abstract Promise delete();

    public abstract Promise open(int flags);

    public abstract Promise close();

    /**
     * Reads data from a file from some offset.
     *
     * todo:
     * The problem is that we can't just pass any buffer. In case of DIO, the buffer
     * needs to be properly aligned. So if there would be a map.get, you can't just start adding
     * the data after.
     *
     * @param offset the offset.
     * @param length the number of bytes to read.
     * @return
     */
    public abstract Promise pread(long offset, int length, long bufferAddress);

    /**
     * @param offset
     * @param length
     * @param bufferAddress
     * @return
     */
    public abstract Promise pwrite(long offset, int length, long bufferAddress);
}
