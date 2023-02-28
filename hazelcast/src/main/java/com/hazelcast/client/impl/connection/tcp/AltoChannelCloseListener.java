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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelCloseListener;

/**
 * A listener to notify pending invocations that are sent
 * over the Alto channels with exception, in case the Alto
 * channel is closed.
 * <p>
 * The invocations that will be notified with this listener
 * are the ones that are sent with the ClientConnection adapter
 * of the Alto channel, such as heartbeats.
 */
public class AltoChannelCloseListener implements ChannelCloseListener {
    private final HazelcastClientInstanceImpl client;
    public AltoChannelCloseListener(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public void onClose(Channel channel) {
        ClientConnection adapter = (ClientConnection) channel.attributeMap().get(AltoChannelClientConnectionAdapter.class);
        assert adapter != null;

        client.getInvocationService().onConnectionClose(adapter);
    }
}
