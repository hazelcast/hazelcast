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

import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import com.hazelcast.internal.tpcengine.Option;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount"})
public class IOUringAsyncSocketOptions implements AsyncSocketOptions {

    private final LinuxSocket nativeSocket;

    IOUringAsyncSocketOptions(LinuxSocket nativeSocket) {
        this.nativeSocket = nativeSocket;
    }

    @Override
    public boolean isSupported(Option option) {
        if (AsyncSocketOptions.TCP_NODELAY.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.SO_RCVBUF.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.SO_SNDBUF.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.SO_KEEPALIVE.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.SO_REUSEADDR.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.TCP_KEEPCOUNT.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.TCP_KEEPINTERVAL.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.TCP_KEEPIDLE.equals(option)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public <T> T getIfSupported(Option<T> option) {
        checkNotNull(option, "option");

        try {
            if (AsyncSocketOptions.TCP_NODELAY.equals(option)) {
                return (T) (Boolean) nativeSocket.isTcpNoDelay();
            } else if (AsyncSocketOptions.SO_RCVBUF.equals(option)) {
                return (T) (Integer) nativeSocket.getReceiveBufferSize();
            } else if (AsyncSocketOptions.SO_SNDBUF.equals(option)) {
                return (T) (Integer) nativeSocket.getSendBufferSize();
            } else if (AsyncSocketOptions.SO_KEEPALIVE.equals(option)) {
                return (T) (Boolean) nativeSocket.isKeepAlive();
            } else if (AsyncSocketOptions.SO_REUSEADDR.equals(option)) {
                return (T) (Boolean) nativeSocket.isReuseAddress();
            } else if (AsyncSocketOptions.TCP_KEEPCOUNT.equals(option)) {
                return (T) (Integer) nativeSocket.getTcpKeepaliveProbes();
            } else if (AsyncSocketOptions.TCP_KEEPINTERVAL.equals(option)) {
                return (T) (Integer) nativeSocket.getTcpKeepaliveIntvl();
            } else if (AsyncSocketOptions.TCP_KEEPIDLE.equals(option)) {
                return (T) (Integer) nativeSocket.getTcpKeepAliveTime();
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to getOption [" + option.name() + "]", e);
        }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public <T> boolean setIfSupported(Option<T> option, T value) {
        checkNotNull(option, "option");
        checkNotNull(value, "value");

        try {
            if (AsyncSocketOptions.TCP_NODELAY.equals(option)) {
                nativeSocket.setTcpNoDelay((Boolean) value);
                return true;
            } else if (AsyncSocketOptions.SO_RCVBUF.equals(option)) {
                nativeSocket.setReceiveBufferSize((Integer) value);
                return true;
            } else if (AsyncSocketOptions.SO_SNDBUF.equals(option)) {
                nativeSocket.setSendBufferSize((Integer) value);
                return true;
            } else if (AsyncSocketOptions.SO_KEEPALIVE.equals(option)) {
                nativeSocket.setKeepAlive((Boolean) value);
                return true;
            } else if (AsyncSocketOptions.SO_REUSEADDR.equals(option)) {
                nativeSocket.setReuseAddress((Boolean) value);
                return true;
//            } else if (SO_TIMEOUT.equals(option)) {
//                nativeSocket.setSoTimeout((Integer) value);
            } else if (AsyncSocketOptions.TCP_KEEPCOUNT.equals(option)) {
                nativeSocket.setTcpKeepAliveProbes((Integer) value);
                return true;
            } else if (AsyncSocketOptions.TCP_KEEPIDLE.equals(option)) {
                nativeSocket.setTcpKeepAliveTime((Integer) value);
                return true;
            } else if (AsyncSocketOptions.TCP_KEEPINTERVAL.equals(option)) {
                nativeSocket.setTcpKeepaliveIntvl((Integer) value);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to setOption [" + option.name() + "] with value [" + value + "]", e);
        }
    }
}
