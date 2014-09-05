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

package com.hazelcast.collection.set;

import com.hazelcast.collection.AbstractCollectionProxyImpl;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.core.ISet;
import com.hazelcast.spi.NodeEngine;

public class SetProxyImpl<E> extends AbstractCollectionProxyImpl<SetService, E> implements ISet<E> {

    public SetProxyImpl(String name, NodeEngine nodeEngine, SetService service) {
        super(name, nodeEngine, service);
    }

    @Override
    protected CollectionConfig getConfig(NodeEngine nodeEngine) {
        return nodeEngine.getConfig().findSetConfig(name);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

}
