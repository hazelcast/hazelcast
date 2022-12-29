package com.hazelcast.config.alto;

import com.hazelcast.config.InvalidConfigurationException;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.regex.Pattern;

public class AltoSocketConfig {
    private static final String DEFAULT_PORT_RANGE = "11000-21000";
    private static final int DEFAULT_RECEIVE_BUFFER_SIZE = 128 * 1024;
    private static final int DEFAULT_SEND_BUFFER_SIZE = 128 * 1024;

    private String portRange = DEFAULT_PORT_RANGE;
    private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
    private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;

    public String getPortRange() {
        return portRange;
    }

    public AltoSocketConfig setPortRange(@Nonnull String portRange) {
        if (!Bounds.PORT_RANGE_PATTERN.matcher(portRange).matches()) {
            throw new InvalidConfigurationException("Invalid port definition");
        }

        this.portRange = portRange;
        return this;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public AltoSocketConfig setReceiveBufferSize(int receiveBufferSize) {
        if (receiveBufferSize < Bounds.MIN_BUFFER_SIZE || receiveBufferSize > Bounds.MAX_BUFFER_SIZE) {
            throw new InvalidConfigurationException("Buffer size should be between "
                    + Bounds.MIN_BUFFER_SIZE + " and " + Bounds.MAX_BUFFER_SIZE);
        }

        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public AltoSocketConfig setSendBufferSize(int sendBufferSize) {
        if (sendBufferSize < Bounds.MIN_BUFFER_SIZE || sendBufferSize > Bounds.MAX_BUFFER_SIZE) {
            throw new InvalidConfigurationException("Buffer size should be between "
                    + Bounds.MIN_BUFFER_SIZE + " and " + Bounds.MAX_BUFFER_SIZE);
        }

        this.sendBufferSize = sendBufferSize;
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
        return receiveBufferSize == that.receiveBufferSize
                && sendBufferSize == that.sendBufferSize
                && portRange.equals(that.portRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(portRange, receiveBufferSize, sendBufferSize);
    }

    @Override
    public String toString() {
        return "AltoSocketConfig{"
                + "portRange='" + portRange + '\''
                + ", receiveBufferSize=" + receiveBufferSize
                + ", sendBufferSize=" + sendBufferSize
                + '}';
    }

    private static class Bounds {
        private static final Pattern PORT_RANGE_PATTERN = Pattern.compile("\\d{1,5}-\\d{1,5}");
        private static final int MIN_BUFFER_SIZE = 1 << 15;
        private static final int MAX_BUFFER_SIZE = 1 << 30;
    }
}
