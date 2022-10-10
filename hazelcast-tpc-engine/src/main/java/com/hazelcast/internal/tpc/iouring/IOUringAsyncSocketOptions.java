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

import com.hazelcast.internal.tpc.AsyncSocketOptions;
import com.hazelcast.internal.tpc.Option;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount"})
public class IOUringAsyncSocketOptions implements AsyncSocketOptions {

    private final NativeSocket nativeSocket;

    IOUringAsyncSocketOptions(NativeSocket nativeSocket) {
        this.nativeSocket = nativeSocket;
    }

    @Override
    public <T> T get(Option<T> option) {
        checkNotNull(option, "option");

        try {
            if (TCP_NODELAY.equals(option)) {
                return (T) (Boolean) nativeSocket.isTcpNoDelay();
            } else if (SO_RCVBUF.equals(option)) {
                return (T) (Integer) nativeSocket.getReceiveBufferSize();
            } else if (SO_SNDBUF.equals(option)) {
                return (T) (Integer) nativeSocket.getSendBufferSize();
            } else if (SO_KEEPALIVE.equals(option)) {
                return (T) (Boolean) nativeSocket.isKeepAlive();
            } else if (SO_REUSEADDR.equals(option)) {
                return (T) (Boolean) nativeSocket.isReuseAddress();
            } else if (TCP_KEEPCOUNT.equals(option)) {
                return (T) (Integer) nativeSocket.getTcpKeepaliveProbes();
            } else if (TCP_KEEPINTERVAL.equals(option)) {
                return (T) (Integer) nativeSocket.getTcpKeepaliveIntvl();
            } else if (TCP_KEEPIDLE.equals(option)) {
                return (T) (Integer) nativeSocket.getTcpKeepAliveTime();
            } else {
                throw new UnsupportedOperationException("Unrecognized option:" + option);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to getOption [" + option.name() + "]", e);
        }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public <T> void set(Option<T> option, T value) {
        checkNotNull(option, "option");
        checkNotNull(value, "value");

        try {
            if (TCP_NODELAY.equals(option)) {
                nativeSocket.setTcpNoDelay((Boolean) value);
            } else if (SO_RCVBUF.equals(option)) {
                nativeSocket.setReceiveBufferSize((Integer) value);
            } else if (SO_SNDBUF.equals(option)) {
                nativeSocket.setSendBufferSize((Integer) value);
            } else if (SO_KEEPALIVE.equals(option)) {
                nativeSocket.setKeepAlive((Boolean) value);
            } else if (SO_REUSEADDR.equals(option)) {
                nativeSocket.setReuseAddress((Boolean) value);
//            } else if (SO_TIMEOUT.equals(option)) {
//                nativeSocket.setSoTimeout((Integer) value);
            } else if (TCP_KEEPCOUNT.equals(option)) {
                nativeSocket.setTcpKeepAliveProbes((Integer) value);
            } else if (TCP_KEEPIDLE.equals(option)) {
                nativeSocket.setTcpKeepAliveTime((Integer) value);
            } else if (TCP_KEEPINTERVAL.equals(option)) {
                nativeSocket.setTcpKeepaliveIntvl((Integer) value);
            } else {
                throw new UnsupportedOperationException("Unrecognized option:" + option);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to setOption [" + option.name() + "] with value [" + value + "]", e);
        }
    }
}
