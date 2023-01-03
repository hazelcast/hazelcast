package com.hazelcast.config.alto;

import com.hazelcast.config.InvalidConfigurationException;

import javax.annotation.Nonnull;
import java.util.Objects;

public class AltoSocketConfig {
    private String portRange = "11000-21000";
    private int receiveBufferSizeKb = 128;
    private int sendBufferSizeKb = 128;

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

    public int getReceiveBufferSizeKb() {
        return receiveBufferSizeKb;
    }

    public AltoSocketConfig setReceiveBufferSizeKb(int receiveBufferSizeKb) {
        if (receiveBufferSizeKb < 32 || receiveBufferSizeKb > 1_048_576) {
            throw new InvalidConfigurationException("Buffer size should be between " + 32 + " and " + 1_048_576);
        }

        this.receiveBufferSizeKb = receiveBufferSizeKb;
        return this;
    }

    public int getSendBufferSizeKb() {
        return sendBufferSizeKb;
    }

    public AltoSocketConfig setSendBufferSizeKb(int sendBufferSizeKb) {
        if (sendBufferSizeKb < 32 || sendBufferSizeKb > 1_048_576) {
            throw new InvalidConfigurationException("Buffer size should be between " + 32 + " and " + 1_048_576);
        }

        this.sendBufferSizeKb = sendBufferSizeKb;
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
        return receiveBufferSizeKb == that.receiveBufferSizeKb
                && sendBufferSizeKb == that.sendBufferSizeKb
                && portRange.equals(that.portRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(portRange, receiveBufferSizeKb, sendBufferSizeKb);
    }

    @Override
    public String toString() {
        return "AltoSocketConfig{"
                + "portRange='" + portRange + '\''
                + ", receiveBufferSizeKb=" + receiveBufferSizeKb
                + ", sendBufferSizeKb=" + sendBufferSizeKb
                + '}';
    }
}
