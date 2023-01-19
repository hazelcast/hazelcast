package com.hazelcast.config.alto;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Socket configuration for Alto. In Alto, each eventloop has its own
 * sockets.
 *
 * @see AltoConfig
 * @since 5.3
 */
@Beta
public class AltoSocketConfig {
    private String portRange = "11000-21000";
    private int receiveBufferSizeKB = 128;
    private int sendBufferSizeKB = 128;

    /**
     * Gets the possible port range for Alto sockets to bind.
     *
     * @return the port range string
     */
    @Nonnull
    public String getPortRange() {
        return portRange;
    }

    /**
     * Sets the possible port range for Alto sockets to bind.
     *
     * @param portRange the port range to set
     * @return this Alto socket config
     * @throws IllegalArgumentException if portRange doesn't match {@code
     *                                  \d{1,5}-\d{1,5}} regular expression
     *                                  or null
     */
    @Nonnull
    public AltoSocketConfig setPortRange(@Nonnull String portRange) {
        checkNotNull(portRange);
        if (!portRange.matches("\\d{1,5}-\\d{1,5}")) {
            throw new IllegalArgumentException("Invalid port range");
        }

        this.portRange = portRange;
        return this;
    }

    /**
     * Gets the receive-buffer size of the Alto sockets in kilobytes.
     *
     * @return the receive-buffer size of the Alto sockets in kilobytes
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    public int getReceiveBufferSizeKB() {
        return receiveBufferSizeKB;
    }

    /**
     * Sets the receive-buffer size of the Alto sockets in kilobytes.
     *
     * @param receiveBufferSizeKB the receive-buffer size of the Alto sockets in kilobytes
     * @return this Alto socket config
     * @throws IllegalArgumentException if receiveBufferSizeKB isn't positive
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    @Nonnull
    public AltoSocketConfig setReceiveBufferSizeKB(int receiveBufferSizeKB) {
        this.receiveBufferSizeKB = checkPositive("receiveBufferSizeKB", receiveBufferSizeKB);
        return this;
    }

    /**
     * Gets the send-buffer size of the Alto sockets in kilobytes.
     *
     * @return the send-buffer size of the Alto sockets in kilobytes
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    public int getSendBufferSizeKB() {
        return sendBufferSizeKB;
    }

    /**
     * Sets the send-buffer size of the Alto sockets in kilobytes.
     *
     * @param sendBufferSizeKB the send-buffer size of the Alto sockets in kilobytes
     * @return this Alto socket config
     * @throws IllegalArgumentException if sendBufferSizeKB isn't positive
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    @Nonnull
    public AltoSocketConfig setSendBufferSizeKB(int sendBufferSizeKB) {
        this.sendBufferSizeKB = checkPositive("sendBufferSizeKB", sendBufferSizeKB);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AltoSocketConfig that = (AltoSocketConfig) o;
        return receiveBufferSizeKB == that.receiveBufferSizeKB
                && sendBufferSizeKB == that.sendBufferSizeKB
                && portRange.equals(that.portRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(portRange, receiveBufferSizeKB, sendBufferSizeKB);
    }

    @Override
    public String toString() {
        return "AltoSocketConfig{"
                + "portRange='" + portRange + '\''
                + ", receiveBufferSizeKB=" + receiveBufferSizeKB
                + ", sendBufferSizeKB=" + sendBufferSizeKB
                + '}';
    }
}
