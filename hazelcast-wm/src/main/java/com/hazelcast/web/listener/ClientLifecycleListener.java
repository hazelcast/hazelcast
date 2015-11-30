/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web.listener;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.web.ClusteredSessionService;

import static com.hazelcast.core.LifecycleEvent.LifecycleState;

/**
 * Listens Lifecycle Events for client instance, if client is in shutdown state
 * {@link ClusteredSessionService} is notified with failed connection
 */

public class ClientLifecycleListener implements LifecycleListener {

    private final ClusteredSessionService sessionService;

    public ClientLifecycleListener(ClusteredSessionService sessionService) {
        this.sessionService = sessionService;
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        if (event.getState().equals(LifecycleState.SHUTDOWN)) {
            sessionService.setFailedConnection(true);
        }
    }
}
