/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.statemachine;

import com.hazelcast.jet.impl.container.ContainerResponse;
import com.hazelcast.jet.impl.container.ContainerEvent;
import com.hazelcast.jet.impl.container.ContainerState;

/**
 * Represents abstract container state-machine;
 *
 * @param <I> - type of the input event;
 * @param <S> - type of the state;
 * @param <O> - type of the state-machine output;
 */
public interface ContainerStateMachine
        <I extends ContainerEvent,
                S extends ContainerState,
                O extends ContainerResponse> extends StateMachine<I, S, O> {
}
