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

package com.hazelcast.jet.internal.api.statemachine;

/**
 * Used to be sent and raise state-machine's transition;
 *
 * @param <E> - type of the state-machine's event;
 * @param <P> - type of the argument pauLoad;
 */
public interface StateMachineRequest<E extends StateMachineEvent, P> {
    /**
     * @return - event of the container;
     */
    E getContainerEvent();

    /**
     * @return - payLoad;
     */
    P getPayLoad();
}
