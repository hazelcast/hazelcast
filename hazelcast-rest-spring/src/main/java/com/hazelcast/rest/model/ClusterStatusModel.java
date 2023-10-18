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

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterStatusModel {
    @JsonProperty("members")
    private final List<MemberDetailModel> members;
    @JsonProperty("connectionCount")
    private final int connectionCount;
    @JsonProperty("allConnectionCount")
    private final int allConnectionCount;

    public ClusterStatusModel(List<MemberDetailModel> members, int connectionCount, int allConnectionCount) {
        this.members = members;
        this.connectionCount = connectionCount;
        this.allConnectionCount = allConnectionCount;
    }

    public static class ClusterStatusModelBuilder {
        private List<MemberDetailModel> members;
        private int connectionCount;
        private int allConnectionCount;

        public ClusterStatusModelBuilder() {
        }

        public ClusterStatusModelBuilder members(List<MemberDetailModel> members) {
            this.members = members;
            return this;
        }

        public ClusterStatusModelBuilder connectionCount(int connectionCount) {
            this.connectionCount = connectionCount;
            return this;
        }

        public ClusterStatusModelBuilder allConnectionCount(int allConnectionCount) {
            this.allConnectionCount = allConnectionCount;
            return this;
        }

        public ClusterStatusModel build() {
            return new ClusterStatusModel(this.members, this.connectionCount, this.allConnectionCount);
        }
    }
}
