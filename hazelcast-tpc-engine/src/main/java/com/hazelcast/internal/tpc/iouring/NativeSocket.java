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

package com.hazelcast.internal.tpc.iouring;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static com.hazelcast.internal.tpc.iouring.Linux.SOCK_NONBLOCK;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * A Linux Socket.
 * <p>
 * This class is not thread-safe.
 * <p>
 * https://web.archive.org/web/20141009130845/https://rkennke.wordpress.com/2007/07/30/efficient-jni-programming-iv-wrapping-native-data-objects/
 * https://web.archive.org/web/20080831123351/http://kennke.org/blog/2007/07/20/efficient-jni-programming-part-i/
 * https://web.archive.org/web/20080920074705/http://kennke.org/blog/2007/07/24/efficient-jni-programming-ii-field-and-method-access/
 * https://web.archive.org/web/20090816180817/http://kennke.org/blog/2007/07/28/efficient-jni-programming-iii-array-access/
 */
public final class NativeSocket implements AutoCloseable {

    static {
        IOUringLibrary.ensureAvailable();
        initNative();
    }

    // https://students.mimuw.edu.pl/SO/Linux/Kod/include/linux/socket.h.html
    public final static int AF_INET = 2;
    public final static int AF_INET6 = 10;
    public final static int SOCK_STREAM = 1;
    //public final static int SIZEOF_SOCKADDR_STORAGE;

    public static NativeSocket openTcpIpv4Socket() {
        int family = AF_INET;
        int res = socket(family, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (res < 0) {
            throw new UncheckedIOException(new IOException("Failed to open a socket. " + Linux.strerror(-res)));
        }
        return new NativeSocket(res, family);
    }

    private static native void initNative();

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

    private static native int bind(int socketfd, byte[] address, int port) throws IOException;

    private static native void connect(int socketfd, byte[] address, int port, int family);

    private static native void setBlocking(int fd, boolean blocking);

    private static native boolean isBlocking(int fd);

    private static native InetSocketAddress getLocalAddress(int socketfd);

    private static native InetSocketAddress getRemoteAddress(int socketfd);

    private static native void setTcpNoDelay(int socketFd, boolean tcpNoDelay) throws IOException;

    private static native boolean isTcpNoDelay(int socketFd) throws IOException;

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

    private final int fd;
    private boolean closed;
    private final int addressFamily;

    public NativeSocket(int fd, int addressFamily) {
        this.fd = fd;
        this.addressFamily = addressFamily;
    }

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
     * Does a blocking connects the socket to the specified address.
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

    public void setTcpNoDelay(boolean tcpNoDelay) throws IOException {
        setTcpNoDelay(fd, tcpNoDelay);
    }

    public boolean isTcpNoDelay() throws IOException {
        return isTcpNoDelay(fd);
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

    public void listen(int backlog) {
        listen(fd, backlog);
    }

    public boolean isOpen() {
        return !closed;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Linux.close(fd);
    }
}
