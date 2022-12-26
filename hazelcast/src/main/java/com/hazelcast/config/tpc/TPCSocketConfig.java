package com.hazelcast.config.tpc;

import com.hazelcast.config.InvalidConfigurationException;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Pattern;

public class TPCSocketConfig {
    private static final Collection<String> DEFAULT_PORT_DEFINITIONS = Collections.singleton("11000-21000");
    private static final int DEFAULT_RECEIVE_BUFFER_SIZE = 128 * 1024;
    private static final int DEFAULT_SEND_BUFFER_SIZE = 128 * 1024;

    private Collection<String> portDefinitions = DEFAULT_PORT_DEFINITIONS;
    private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
    private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;

    public Collection<String> getPortDefinitions() {
        return portDefinitions;
    }

    public TPCSocketConfig addPortDefinition(@Nonnull String portDefinition) {
        if (!Bounds.PORT_DEFINITION_PATTERN.matcher(portDefinition).matches()) {
            throw new InvalidConfigurationException("Invalid port definition");
        }

        try {
            portDefinitions.add(portDefinition);
        } catch (UnsupportedOperationException e) {
            // since user modified port definitions, remove default range
            portDefinitions = new HashSet<>();
            portDefinitions.add(portDefinition);
        }
        return this;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public TPCSocketConfig setReceiveBufferSize(int receiveBufferSize) {
        if (receiveBufferSize > Bounds.MAX_BUFFER_SIZE || receiveBufferSize < Bounds.MIN_BUFFER_SIZE) {
            throw new InvalidConfigurationException("Buffer size should be between "
                    + Bounds.MIN_BUFFER_SIZE + " and " + Bounds.MAX_BUFFER_SIZE);
        }

        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public TPCSocketConfig setSendBufferSize(int sendBufferSize) {
        if (sendBufferSize > Bounds.MAX_BUFFER_SIZE || sendBufferSize < Bounds.MIN_BUFFER_SIZE) {
            throw new InvalidConfigurationException("Buffer size should be between "
                    + Bounds.MIN_BUFFER_SIZE + " and " + Bounds.MAX_BUFFER_SIZE);
        }

        this.sendBufferSize = sendBufferSize;
        return this;
    }

    private static class Bounds {
        private static final Pattern PORT_DEFINITION_PATTERN = Pattern.compile("\\d{1,5}-\\d{1,5}");
        private static final int MIN_BUFFER_SIZE = 1 << 15;
        private static final int MAX_BUFFER_SIZE = 1 << 30;
    }
}
