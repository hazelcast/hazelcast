/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.dto;

import com.hazelcast.cp.CPMember;

/**
 * A DTO that describes CP member description sent to Management Center.
 *
 * @see CPMember
 */
public final class CPMemberDTO {

    private final String uuid;
    private final String address;

    public CPMemberDTO(String uuid, String address) {
        this.uuid = uuid;
        this.address = address;
    }

    public String getUuid() {
        return uuid;
    }

    public String getAddress() {
        return address;
    }

}
