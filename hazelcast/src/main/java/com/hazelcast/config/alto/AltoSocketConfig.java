package com.hazelcast.config.alto;

import com.hazelcast.config.InvalidConfigurationException;

import javax.annotation.Nonnull;
import java.util.Objects;

public class AltoSocketConfig {
    private String portRange = "11000-21000";
    private int receiveBufferSizeKB = 128;
    private int sendBufferSizeKB = 128;

    public String getPortRange() {
        return portRange;
    }

    public AltoSocketConfig setPortRange(@Nonnull String portRange) {
        if (!portRange.matches("\\d{1,5}-\\d{1,5}")) {
            throw new InvalidConfigurationException("Invalid port range");
        }

        this.portRange = portRange;
        return this;
    }

    public int getReceiveBufferSizeKB() {
        return receiveBufferSizeKB;
    }

    public AltoSocketConfig setReceiveBufferSizeKB(int receiveBufferSizeKB) {
        if (receiveBufferSizeKB < 32 || receiveBufferSizeKB > 1_048_576) {
            throw new InvalidConfigurationException("Buffer size should be between " + 32 + " and " + 1_048_576);
        }

        this.receiveBufferSizeKB = receiveBufferSizeKB;
        return this;
    }

    public int getSendBufferSizeKB() {
        return sendBufferSizeKB;
    }

    public AltoSocketConfig setSendBufferSizeKB(int sendBufferSizeKB) {
        if (sendBufferSizeKB < 32 || sendBufferSizeKB > 1_048_576) {
            throw new InvalidConfigurationException("Buffer size should be between " + 32 + " and " + 1_048_576);
        }

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
