/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iouring;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static com.hazelcast.internal.tpcengine.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpcengine.iouring.Linux.newSysCallFailedException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A JNI wrapper around a Linux Socket.
 */
@SuppressWarnings({"checkstyle:LineLength", "checkstyle:MethodCount"})
public final class LinuxSocket implements AutoCloseable {

    static {
        UringLibrary.ensureAvailable();
        initNative();

        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            CLOSED = l.findVarHandle(LinuxSocket.class, "closed", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // https://students.mimuw.edu.pl/SO/Linux/Kod/include/linux/socket.h.html
    public static final int AF_INET = 2;
    public static final int AF_INET6 = 10;
    public static final int SOCK_STREAM = 1;

    private static final VarHandle CLOSED;

    private final int fd;
    private volatile boolean closed;
    private final int addressFamily;

    public LinuxSocket(int fd, int addressFamily) {
        this.fd = checkPositive(fd, "fd");
        this.addressFamily = addressFamily;
    }

    /**
     * Creates a non blocking TCP/IPv4 socket.
     *
     * @return the created LinuxSocket.
     */
    public static LinuxSocket createNonBlockingTcpIpv4Socket() {
        int family = AF_INET;
        int res = socket(family, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (res < 0) {
            throw newSysCallFailedException("Failed to create a socket.", "socket(2)", -res);
        }
        return new LinuxSocket(res, family);
    }

    public static LinuxSocket createBlockingTcpIpv4Socket() {
        int family = AF_INET;
        int res = socket(family, SOCK_STREAM, 0);
        if (res < 0) {
            throw newSysCallFailedException("Failed to create a socket.", "socket(2)", -res);
        }
        return new LinuxSocket(res, family);
    }

    private static native void initNative();

    /**
     * https://man7.org/linux/man-pages/man2/listen.2.html
     */
    private static native void listen(int sockfd, int backlog);

    /**
     * https://man7.org/linux/man-pages/man2/socket.2.html
     *
     * @param domain
     * @param type
     * @param protocol
     * @return the filedescriptor.
     */
    private static native int socket(int domain, int type, int protocol);

    // todo: fix names
    public static native InetSocketAddress toInetSocketAddress(long ptr, long length);

    /**
     * https://man7.org/linux/man-pages/man2/bind.2.html
     */
    private static native int bind(int socketfd, byte[] address, int port) throws IOException;

    /**
     * https://man7.org/linux/man-pages/man2/connect.2.html
     */
    private static native void connect(int socketfd, byte[] address, int port, int family);

    private static native void setBlocking(int fd, boolean blocking);

    private static native boolean isBlocking(int fd);

    private static native InetSocketAddress getLocalAddress(int socketfd);

    private static native InetSocketAddress getRemoteAddress(int socketfd);

    private static native void setTcpNoDelay(int socketFd, boolean enabled) throws IOException;

    private static native boolean isTcpNoDelay(int socketFd) throws IOException;

    private static native void setTcpQuickAck(int socketFd, boolean enabled) throws IOException;

    private static native boolean isTcpQuickAck(int socketFd) throws IOException;

    private static native void setTcpKeepAliveTime(int socketFd, int keepAliveTime) throws IOException;

    private static native int getTcpKeepAliveTime(int socketFd) throws IOException;

    private static native void setTcpKeepaliveIntvl(int socketFd, int keepaliveIntvl) throws IOException;

    private static native int getTcpKeepaliveIntvl(int socketFd) throws IOException;

    private static native void setTcpKeepAliveProbes(int socketFd, int keepAliveProbes) throws IOException;

    private static native int getTcpKeepaliveProbes(int socketFd) throws IOException;

    private static native void setSendBufferSize(int socketFd, int sendBufferSize) throws IOException;

    private static native int getSendBufferSize(int socketFd) throws IOException;

    private static native void setReceiveBufferSize(int socketFd, int receiveBufferSize) throws IOException;

    private static native int getReceiveBufferSize(int socketFd) throws IOException;

    private static native void setReuseAddress(int socketFd, boolean reuseAddress) throws IOException;

    private static native boolean isReuseAddress(int socketFd) throws IOException;

    private static native void setReusePort(int socketFd, boolean reusePort) throws IOException;

    private static native boolean isReusePort(int socketFd) throws IOException;

    private static native void setKeepAlive(int socketFd, boolean keepAlive) throws IOException;

    private static native boolean isKeepAlive(int socketFd) throws IOException;

    private static native void setSoLinger(int socketFd, int soLinger) throws IOException;

    private static native int getSoLinger(int socketFd) throws IOException;

    public int getAddressFamily() {
        return addressFamily;
    }

    /**
     * Returns the file descriptor of this socket.
     *
     * @return the file descriptor.
     */
    public int fd() {
        return fd;
    }

    /**
     * Configures the socket as blocking.
     *
     * @param blocking
     */
    public void setBlocking(boolean blocking) {
        setBlocking(fd, blocking);
    }

    public boolean isBlocking() {
        return isBlocking(fd);
    }

    /**
     * Does a blocking connect to the specified address.
     *
     * @param address
     * @return
     */
    public boolean connect(SocketAddress address) throws IOException {
        checkNotNull(address, "address");

        if (address instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
            if (!(inetSocketAddress.getAddress() instanceof Inet4Address)) {
                throw new IOException("Only Inet4Address allowed");
            }
            boolean blocking = isBlocking();
            setBlocking(true);
            connect(fd, inetSocketAddress.getAddress().getAddress(), inetSocketAddress.getPort(), addressFamily);

            if (!blocking) {
                setBlocking(false);
            }
            return true;
        } else {
            throw new IOException("Unhandled address:" + address);
        }
    }

    public InetSocketAddress getRemoteAddress() {
        return getRemoteAddress(fd);
    }

    public InetSocketAddress getLocalAddress() {
        return getLocalAddress(fd);
    }

    public void setTcpNoDelay(boolean enabled) throws IOException {
        setTcpNoDelay(fd, enabled);
    }

    public boolean isTcpNoDelay() throws IOException {
        return isTcpNoDelay(fd);
    }

    public void setTcpQuickAck(boolean enabled)throws IOException {
        setTcpQuickAck(fd, enabled);
    }

    public boolean isTcpQuickAck()throws IOException {
        return isTcpQuickAck(fd);
    }

    public int getSendBufferSize() throws IOException {
        return getSendBufferSize(fd);
    }

    public void setSendBufferSize(int sendBufferSize) throws IOException {
        setSendBufferSize(fd, checkPositive(sendBufferSize, "sendBufferSize"));
    }

    public int getReceiveBufferSize() throws IOException {
        return getReceiveBufferSize(fd);
    }

    public void setReceiveBufferSize(int receiveBufferSize) throws IOException {
        setReceiveBufferSize(fd, checkPositive(receiveBufferSize, "receiveBufferSize"));
    }

    public void setReuseAddress(boolean reuseAddress) throws IOException {
        setReuseAddress(fd, reuseAddress);
    }

    public boolean isReuseAddress() throws IOException {
        return isReuseAddress(fd);
    }

    public void setReusePort(boolean reusePort) throws IOException {
        setReusePort(fd, reusePort);
    }

    public boolean isReusePort() throws IOException {
        return isReusePort(fd);
    }

    public void setSoLinger(int soLinger) throws IOException {
        setSoLinger(fd, soLinger);
    }

    public int getSoLinger() throws IOException {
        return getSoLinger(fd);
    }

    public void setKeepAlive(boolean keepAlive) throws IOException {
        setKeepAlive(fd, keepAlive);
    }

    public boolean isKeepAlive() throws IOException {
        return isKeepAlive(fd);
    }

    public void setTcpKeepAliveTime(int keepAliveTime) throws IOException {
        setTcpKeepAliveTime(fd, checkPositive(keepAliveTime, "keepAliveTime"));
    }

    public int getTcpKeepAliveTime() throws IOException {
        return getTcpKeepAliveTime(fd);
    }

    public void setTcpKeepaliveIntvl(int keepaliveIntvl) throws IOException {
        setTcpKeepaliveIntvl(fd, checkPositive(keepaliveIntvl, "keepaliveIntvl"));
    }

    public int getTcpKeepaliveIntvl() throws IOException {
        return getTcpKeepaliveIntvl(fd);
    }

    public void setTcpKeepAliveProbes(int keepAliveProbes) throws IOException {
        setTcpKeepAliveProbes(fd, checkPositive(keepAliveProbes, "keepAliveProbes"));
    }

    public int getTcpKeepaliveProbes() throws IOException {
        return getTcpKeepaliveProbes(fd);
    }

    public void bind(SocketAddress address) throws IOException {
        checkNotNull(address, "address");

        if (address instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
            if (!(inetSocketAddress.getAddress() instanceof Inet4Address)) {
                throw new IOException("Only Inet4Address allowed");
            }

            bind(fd, inetSocketAddress.getAddress().getAddress(), inetSocketAddress.getPort());
        } else {
            throw new IOException("Unhandled address:" + address);
        }
    }

    /**
     * https://man7.org/linux/man-pages/man2/listen.2.html
     */
    public void listen(int backlog) {
        listen(fd, backlog);
    }

    /**
     * Checks if the socket is closed.
     * <p/>
     * This method is thread-safe.
     *
     * @return
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Tries to set the closed flag on the socket.
     * <p/>
     * This method is thread-safe.
     * <p/>
     * The reason this method exists, is to close the socket with IORING_OP_CLOSE
     * instead of {@link #close()}.
     *
     * @return true if the marking was a success, false otherwise.
     */
    public boolean trySetClosed() {
        return CLOSED.compareAndSet(this, false, true);
    }

    /**
     * Closes the socket if it isn't already closed. If already closed,
     * the call is ignored.
     * <p>
     * On success, close(2) is called:
     * https://man7.org/linux/man-pages/man2/close.2.html
     * <p>
     * This method is thread-safe.
     */
    @Override
    public void close() {
        if (trySetClosed()) {
            Linux.close(fd);
        }
    }

    @Override
    public String toString() {
        return "LinuxSocket(fd=" + fd + ")";
    }

}
