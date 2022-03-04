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

package com.hazelcast.internal.nio.ascii;

import static com.hazelcast.internal.util.StringUtil.stringToBytes;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.hazelcast.logging.Logger;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.StringUtil;

/**
 * Test client for verifying text protocols (HTTP REST API, Memcache, ...). Sample usage:
 * <p>
 * <code><pre>
 * TextProtocolClient client = new TextProtocolClient(address, port);
 * client.connect();
 * try {
 *   client.sendData("GET /hazelcast/health/node-state HTTP/1.0\r\n\r\n");
 *   Thread.sleep(1000L);
 *   assertThat(client.getReceivedString(), containsString("ACTIVE"));
 * } finally {
 *   client.close();
 * }
 * </pre></code>
 * <p>
 * This class is not thread safe.
 */
public class TextProtocolClient implements Closeable {

    /**
     * Default timeout for waiting for connection close (from server side).
     *
     * @see #waitUntilClosed()
     */
    private static final long DEFAULT_TIMEOUT_MILLIS = 10 * 1000L;

    private final InetAddress inetAddress;
    private final int port;

    private Socket socket = null;
    private int sentBytesCount = 0;
    private InputStreamConsumerThread isThread = null;
    private Exception exception = null;
    private OutputStream os = null;

    /**
     * Creates new client for given address and port. Client is not connected automatically.
     */
    public TextProtocolClient(InetAddress inetAddress, int port) {
        this.inetAddress = inetAddress;
        this.port = port;
    }

    /**
     * Creates new client for given socket address. Client is not connected automatically.
     */
    public TextProtocolClient(InetSocketAddress inetSocketAdddress) {
        this(inetSocketAdddress.getAddress(), inetSocketAdddress.getPort());
    }

    /**
     * Returns true if the client was connected (and not closed after the connect).
     */
    public boolean isConnected() {
        return socket != null;
    }

    /**
     * Connects the client to a server and starts a new data consuming thread.
     */
    public void connect() {
        try {
            socket = new Socket(inetAddress, port);
            isThread = new InputStreamConsumerThread(socket.getInputStream());
            isThread.start();
            os = socket.getOutputStream();
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to connect to a server.", ioe);
        }
    }

    /**
     * Sends given String (as UTF-8) to the server.
     *
     * @see #sendData(byte[])
     */
    public boolean sendData(String requestDataStr) {
        return sendData(stringToBytes(requestDataStr));
    }

    /**
     * Sends given bytes to the server.
     *
     * @throws IllegalStateException if the client is not connected
     */
    public boolean sendData(byte[] requestData) {
        if (!isConnected()) {
            throw new IllegalStateException("Client is not connected. Call the connect() method first!");
        }
        try {
            os.write(requestData);
            os.flush();
            sentBytesCount += requestData.length;
        } catch (Exception e) {
            exception = e;
            return false;
        }
        return isThread.getException() != null;
    }

    /**
     * Returns {@code true} if the client is not yet connected or the connection was already closed.
     */
    public boolean isConnectionClosed() {
        return isThread != null && !isThread.isAlive();
    }

    /**
     * Waits default timeout ({@value #DEFAULT_TIMEOUT_MILLIS} ms) for closed connection. If the timeout is reached, the
     * program run continues without exception.
     */
    public void waitUntilClosed() throws InterruptedException {
        waitUntilClosed(DEFAULT_TIMEOUT_MILLIS);
    }

    /**
     * Waits given timeout (ms) for closed connection. If the timeout is reached, the
     * program run continues without exception.
     */
    public void waitUntilClosed(long timeoutMillis) throws InterruptedException {
        if (isThread != null) {
            isThread.join(timeoutMillis);
        }
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public int getPort() {
        return port;
    }

    /**
     * Send bytes counter.
     */
    public int getSentBytesCount() {
        return sentBytesCount;
    }

    /**
     * Exceptions holder.
     */
    public Exception getException() {
        return exception != null ? exception : (isThread != null ? isThread.getException() : null);
    }

    /**
     * Returns all bytes received by this client from a server.
     */
    public byte[] getReceivedBytes() {
        return isThread != null ? isThread.getReceivedBytes() : null;
    }

    /**
     * Returns all bytes received by this client from a server as UTF-8 String.
     */
    public String getReceivedString() {
        return StringUtil.bytesToString(getReceivedBytes());
    }

    /**
     * Closes this client and releases resources.
     */
    @Override
    public void close() throws IOException {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException e) {
            Logger.getLogger(TextProtocolClient.class).finest("close failed", e);
        }
        socket = null;
        isThread = null;
        os = null;
        exception = null;
        sentBytesCount = 0;
    }

    private static class InputStreamConsumerThread extends Thread {
        private final InputStream is;
        private final ByteArrayOutputStream os = new ByteArrayOutputStream();
        private volatile IOException exception;

        InputStreamConsumerThread(InputStream is) {
            this.is = is;
        }

        public void run() {
            try {
                IOUtil.drainTo(is, os);
            } catch (IOException e) {
                exception = e;
            }
        }

        public byte[] getReceivedBytes() {
            return os.toByteArray();
        }

        public IOException getException() {
            return exception;
        }
    }

}
