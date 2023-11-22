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

package com.hazelcast.cp.internal.datastructures.cpmap;

/**
 * Exists ONLY to remove the CPMapService.SERVICE_NAME dependency from OS to Enterprise that is induced by the
 * PERMISSION_FACTORY_MAP in {@link com.hazelcast.security.permission.ActionConstants} and the duplication of an important String
 * literal which we don't want to get out of sync.
 */
public class CPMapServiceUtil {
    /**
     * Name that the CPMapService is registered under. Note that this is here to remove the dependency from OS to Enterprise
     * within the key of {@link com.hazelcast.security.permission.ActionConstants}'s PERMISSION_FACTORY_MAP. This should NOT be
     * referenced by any other usage in OS other than this single scenario.
     */
    public static final String SERVICE_NAME = "hz:raft:mapService";
    private CPMapServiceUtil() {
    }
}
