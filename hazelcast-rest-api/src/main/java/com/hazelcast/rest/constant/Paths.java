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
package com.hazelcast.rest.constant;

public class Paths {

    /**
     * Base path
     */
    public static final String V1_BASE_PATH = "/hazelcast/rest/api/v1";
    /**
     * Member base path
     */
    public static final String V1_MEMBER_BASE_PATH = V1_BASE_PATH + "/cluster/members";
    /**
     * Member uuid path
     */
    public static final String V1_MEMBER_UUID_PATH = V1_BASE_PATH + "/cluster/members/{member-uuid}";

    private Paths() {
    }
}
