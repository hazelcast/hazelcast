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

package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;

/**
 * Marker interface for operations those are allowed to be executed or invoked during
 * {@link Node}'s {@link NodeState#PASSIVE} state.
 * <p/>
 * By default, only join, replication and cluster heartbeat operations are allowed during shutdown.
 *
 * @see NodeState
 * @since 3.6
 */
public interface AllowedDuringPassiveState {
}
