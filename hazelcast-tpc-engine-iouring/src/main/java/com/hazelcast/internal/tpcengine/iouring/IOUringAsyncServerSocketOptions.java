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

public class IOUringAsyncServerSocketOptions implements AsyncSocketOptions {

    private final LinuxSocket nativeSocket;

    IOUringAsyncServerSocketOptions(LinuxSocket nativeSocket) {
        this.nativeSocket = nativeSocket;
    }

    @Override
    public boolean isSupported(Option option) {
        checkNotNull(option, "option");

        if (AsyncSocketOptions.SO_RCVBUF.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.SO_REUSEADDR.equals(option)) {
            return true;
        } else if (AsyncSocketOptions.SO_REUSEPORT.equals(option)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public <T> boolean setIfSupported(Option<T> option, T value) {
        checkNotNull(option, "option");
        checkNotNull(value, "value");

        try {
            if (AsyncSocketOptions.SO_RCVBUF.equals(option)) {
                nativeSocket.setReceiveBufferSize((Integer) value);
                return true;
            } else if (AsyncSocketOptions.SO_REUSEADDR.equals(option)) {
                nativeSocket.setReuseAddress((Boolean) value);
                return true;
            } else if (AsyncSocketOptions.SO_REUSEPORT.equals(option)) {
                nativeSocket.setReusePort((Boolean) value);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to set " + option.name() + " with value [" + value + "]", e);
        }
    }

    @Override
    public <T> T getIfSupported(Option<T> option) {
        checkNotNull(option, "option");

        try {
            if (AsyncSocketOptions.SO_RCVBUF.equals(option)) {
                return (T) (Integer) nativeSocket.getReceiveBufferSize();
            } else if (AsyncSocketOptions.SO_REUSEADDR.equals(option)) {
                return (T) (Boolean) nativeSocket.isReuseAddress();
            } else if (AsyncSocketOptions.SO_REUSEPORT.equals(option)) {
                return (T) (Boolean) nativeSocket.isReusePort();
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get option " + option.name(), e);
        }
    }
}
