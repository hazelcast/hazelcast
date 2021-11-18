/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.ViewResolver;
import com.hazelcast.sql.impl.schema.view.View;

import javax.annotation.Nullable;

public class ViewResolverImpl implements ViewResolver {
    private static final String VIEW_STORAGE_NAME = "__sql.catalog";

    private final NodeEngine nodeEngine;

    public ViewResolverImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @SuppressWarnings("ConstantConditions")
    @Nullable
    @Override
    public View resolve(String name) {
        HazelcastInstance instance = nodeEngine.getHazelcastInstance();
        ReplicatedMap<String, View> catalogContainer = instance.getReplicatedMap(VIEW_STORAGE_NAME);
        Object viewCandidate = catalogContainer.get(name);
        if (viewCandidate instanceof View) {
            return (View) viewCandidate;
        } else {
            return null;
        }
    }
}
