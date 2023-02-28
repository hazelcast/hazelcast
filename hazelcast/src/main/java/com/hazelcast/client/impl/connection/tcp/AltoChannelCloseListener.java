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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelCloseListener;

/**
 * A listener to close the client connection in case any
 * of the associated Alto channels are closed.
 */
public class AltoChannelCloseListener implements ChannelCloseListener {
    private final TcpClientConnection connection;

    public AltoChannelCloseListener(TcpClientConnection connection) {
        this.connection = connection;
    }

    @Override
    public void onClose(Channel channel) {
        // A connection might have many channels. The calls to close
        // after the first one are no-op.
        connection.close("The Alto channel: " + channel + " is closed", null);
    }
}
