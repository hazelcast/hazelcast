/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 * Configuration for Wan target replication reference(read only)
 */
public class WanReplicationRefReadOnly extends WanReplicationRef {

    public WanReplicationRefReadOnly(WanReplicationRef ref) {
        super(ref);
    }

    public WanReplicationRef setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public WanReplicationRef setMergePolicy(String mergePolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
