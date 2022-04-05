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

package com.hazelcast.wan;

/**
 * The type of the WAN event, signifies the type of the mutation that occurred
 * that caused the WAN event.
 */
public enum WanEventType {
    /**
     * WAN sync event (carrying a piece of data for synchronization between clusters)
     */
    SYNC,
    /**
     * Add/update event (can be caused by either adding or creating some data)
     */
    ADD_OR_UPDATE,
    /**
     * Remove event (caused by removing data)
     */
    REMOVE
}
