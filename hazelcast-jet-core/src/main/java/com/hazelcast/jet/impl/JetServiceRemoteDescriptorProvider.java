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

package com.hazelcast.jet.impl;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptor;
import com.hazelcast.spi.impl.servicemanager.RemoteServiceDescriptorProvider;

public class JetServiceRemoteDescriptorProvider implements RemoteServiceDescriptorProvider {
    @Override
    public RemoteServiceDescriptor[] createRemoteServiceDescriptors() {
        return new RemoteServiceDescriptor[]{
                new RemoteServiceDescriptor() {
                    @Override
                    public String getServiceName() {
                        return JetService.SERVICE_NAME;
                    }

                    @Override
                    public RemoteService getService(NodeEngine nodeEngine) {
                        return new JetService(nodeEngine);
                    }
                },
        };
    }
}
