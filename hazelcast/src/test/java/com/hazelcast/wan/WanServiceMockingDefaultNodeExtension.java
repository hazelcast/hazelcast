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

package com.hazelcast.wan;

import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.wan.impl.WanReplicationService;

public class WanServiceMockingDefaultNodeExtension extends DefaultNodeExtension {
    private final WanReplicationService wanReplicationService;

    public WanServiceMockingDefaultNodeExtension(Node node,
                                                 WanReplicationService wanReplicationService) {
        super(node);
        this.wanReplicationService = wanReplicationService;
    }

    @Override
    public <T> T createService(Class<T> clazz) {
        return clazz.isAssignableFrom(WanReplicationService.class)
                ? (T) wanReplicationService : super.createService(clazz);
    }
}
