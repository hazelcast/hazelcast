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
package com.hazelcast.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class MemberDetailModel {
    @JsonProperty("address")
    private final String address;
    @JsonProperty("liteMember")
    private final Boolean liteMember;
    @JsonProperty("localMember")
    private final Boolean localMember;
    @JsonProperty("uuid")
    private final String uuid;
    @JsonProperty("memberVersion")
    private final String memberVersion;

    public MemberDetailModel(String address, Boolean liteMember, Boolean localMember, String uuid, String memberVersion) {
        this.address = address;
        this.liteMember = liteMember;
        this.localMember = localMember;
        this.uuid = uuid;
        this.memberVersion = memberVersion;
    }

    public static class MemberDetailModelBuilder {
        private String address;
        private Boolean liteMember;
        private Boolean localMember;
        private String uuid;
        private String memberVersion;
        public MemberDetailModelBuilder() {
        }

        public MemberDetailModelBuilder address(String address) {
            this.address = address;
            return this;
        }

        public MemberDetailModelBuilder liteMember(Boolean liteMember) {
            this.liteMember = liteMember;
            return this;
        }

        public MemberDetailModelBuilder localMember(Boolean localMember) {
            this.localMember = localMember;
            return this;
        }

        public MemberDetailModelBuilder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public MemberDetailModelBuilder memberVersion(String memberVersion) {
            this.memberVersion = memberVersion;
            return this;
        }

        public MemberDetailModel build() {
            return new MemberDetailModel(this.address, this.liteMember, this.localMember, this.uuid, this.memberVersion);
        }
    }
}
