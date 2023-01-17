package com.hazelcast.config.alto;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Socket configuration for Alto. In Alto, each eventloop has its own
 * socket.
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
     * This method gets the possible port range for alto sockets to bind.
     *
     * @return the port range string
     */
    public String getPortRange() {
        return portRange;
    }

    /**
     * This method sets the possible port range for alto sockets to bind.
     *
     * @param portRange the port range to set
     * @return this alto socket config
     */
    public AltoSocketConfig setPortRange(@Nonnull String portRange) {
        checkNotNull(portRange);
        if (!portRange.matches("\\d{1,5}-\\d{1,5}")) {
            throw new IllegalArgumentException("Invalid port range");
        }

        this.portRange = portRange;
        return this;
    }

    /**
     * @return the receive-buffer size of the alto sockets in kilobytes
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    public int getReceiveBufferSizeKB() {
        return receiveBufferSizeKB;
    }

    /**
     * @param receiveBufferSizeKB the receive-buffer size of the alto sockets to set in kilobytes
     * @return this alto socket config
     * @see java.net.SocketOptions#SO_RCVBUF
     */
    public AltoSocketConfig setReceiveBufferSizeKB(int receiveBufferSizeKB) {
        checkPositive("receiveBufferSizeKB", receiveBufferSizeKB);
        this.receiveBufferSizeKB = receiveBufferSizeKB;
        return this;
    }

    /**
     * @return the send-buffer size of the alto sockets in kilobytes
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    public int getSendBufferSizeKB() {
        return sendBufferSizeKB;
    }

    /**
     * @param sendBufferSizeKB the send-buffer size of the alto sockets to set in kilobytes
     * @return this alto socket config
     * @see java.net.SocketOptions#SO_SNDBUF
     */
    public AltoSocketConfig setSendBufferSizeKB(int sendBufferSizeKB) {
        checkPositive("sendBufferSizeKB", sendBufferSizeKB);
        this.sendBufferSizeKB = sendBufferSizeKB;
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
