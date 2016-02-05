/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.hazelcast;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;

import java.util.concurrent.ConcurrentMap;

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.api.JetApplicationManager;
import com.hazelcast.jet.api.application.ApplicationProxy;
import com.hazelcast.jet.impl.application.ApplicationProxyImpl;
import com.hazelcast.jet.impl.application.JetApplicationManagerImpl;

public class JetServiceImpl implements JetService {
    private final NodeEngine nodeEngine;
    private final JetApplicationManager applicationManager;
    private final ConcurrentMap<String, ApplicationProxy> applications;

    private final ConstructorFunction<String, ApplicationProxy> constructor =
            new ConstructorFunction<String, ApplicationProxy>() {
                @Override
                public ApplicationProxy createNew(String name) {
                    return new ApplicationProxyImpl(name, JetServiceImpl.this, nodeEngine);
                }
            };

    public JetServiceImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.applicationManager = new JetApplicationManagerImpl(nodeEngine);
        this.applications = new ConcurrentHashMap<String, ApplicationProxy>();
    }

    @Override
    public JetApplicationManager getApplicationManager() {
        return this.applicationManager;
    }

    @Override
    public ApplicationProxy createDistributedObject(String objectName) {
        return ConcurrencyUtil.getOrPutSynchronized(this.applications, objectName, this.applications, this.constructor);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        ApplicationProxy applicationProxy = this.applications.get(objectName);

        if (applicationProxy != null) {
            applicationProxy.destroy();
        }
    }
}
