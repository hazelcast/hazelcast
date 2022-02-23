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

package com.hazelcast.config;

/**
 * Enum of operation names for handling client permissions when the member is joining into the cluster.
 *
 * @see SecurityConfig
 */
public enum OnJoinPermissionOperationName {

    /**
     * Operation which replaces locally configured permissions by the ones received from cluster.
     */
    RECEIVE,
    /**
     * Operation which refreshes cluster permissions with locally specified ones.
     */
    SEND,
    /**
     * No-op - neither receives nor sends permissions.
     */
    NONE;
}
