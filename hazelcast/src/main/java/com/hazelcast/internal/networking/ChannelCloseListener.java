/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking;

/**
 * A listener called when a {@link Channel} is closed.
 *
 * One of the potential usages is to release resources attached to a channel e.g.
 * deregistration of metrics.
 */
public interface ChannelCloseListener {

    /**
     * Called when the channel is closed.
     *
     * @param channel the channel closed.
     */
    void onClose(Channel channel);
}
