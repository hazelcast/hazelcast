/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.service;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.servicemanager.ServiceDescriptor;
import com.hazelcast.spi.impl.servicemanager.ServiceDescriptorProvider;
import com.hazelcast.vector.internal.impl.VectorCollectionService;

public class VectorCollectionServiceDescriptorProvider implements ServiceDescriptorProvider {

    private static final ServiceDescriptor[] DESCRIPTORS;

    static {
        DESCRIPTORS = new ServiceDescriptor[1];
        DESCRIPTORS[0] = new ServiceDescriptor() {
            @Override
            public String getServiceName() {
                return VectorCollectionService.SERVICE_NAME;
            }

            @Override
            public Object getService(NodeEngine nodeEngine) {
                return new VectorCollectionServiceImpl(nodeEngine);
            }
        };
    }

    @Override
    public ServiceDescriptor[] createServiceDescriptors() {
        return DESCRIPTORS;
    }
}
