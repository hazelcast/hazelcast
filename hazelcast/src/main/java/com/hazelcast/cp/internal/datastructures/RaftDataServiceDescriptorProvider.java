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

package com.hazelcast.cp.internal.datastructures;

import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.servicemanager.ServiceDescriptor;
import com.hazelcast.spi.impl.servicemanager.ServiceDescriptorProvider;

/**
 * Service descriptions of Raft data structures
 */
public class RaftDataServiceDescriptorProvider implements ServiceDescriptorProvider {

    @Override
    public ServiceDescriptor[] createServiceDescriptors() {
        return new ServiceDescriptor[] {
            new RaftAtomicLongServiceDescriptor(),
            new RaftLockServiceDescriptor(),
            new RaftSessionManagerServiceDescriptor(),
            new RaftAtomicRefServiceDescriptor(),
            new RaftSemaphoreServiceDescriptor(),
            new RaftCountDownLatcherviceDescriptor(),
        };
    }

    private static class RaftAtomicLongServiceDescriptor implements ServiceDescriptor {
        @Override
        public String getServiceName() {
            return RaftAtomicLongService.SERVICE_NAME;
        }

        @Override
        public Object getService(NodeEngine nodeEngine) {
            return new RaftAtomicLongService(nodeEngine);
        }
    }

    private static class RaftSessionManagerServiceDescriptor implements ServiceDescriptor {
        @Override
        public String getServiceName() {
            return ProxySessionManagerService.SERVICE_NAME;
        }

        @Override
        public Object getService(NodeEngine nodeEngine) {
            return new ProxySessionManagerService(nodeEngine);
        }
    }

    private static class RaftLockServiceDescriptor implements ServiceDescriptor {
        @Override
        public String getServiceName() {
            return RaftLockService.SERVICE_NAME;
        }

        @Override
        public Object getService(NodeEngine nodeEngine) {
            return new RaftLockService(nodeEngine);
        }
    }

    private static class RaftAtomicRefServiceDescriptor implements ServiceDescriptor {
        @Override
        public String getServiceName() {
            return RaftAtomicRefService.SERVICE_NAME;
        }

        @Override
        public Object getService(NodeEngine nodeEngine) {
            return new RaftAtomicRefService(nodeEngine);
        }
    }

    private static class RaftSemaphoreServiceDescriptor implements ServiceDescriptor {
        @Override
        public String getServiceName() {
                return RaftSemaphoreService.SERVICE_NAME;
        }

        @Override
        public Object getService(NodeEngine nodeEngine) {
            return new RaftSemaphoreService(nodeEngine);
        }
    }

    private static class RaftCountDownLatcherviceDescriptor implements ServiceDescriptor {
        @Override
        public String getServiceName() {
            return RaftCountDownLatchService.SERVICE_NAME;
        }

        @Override
        public Object getService(NodeEngine nodeEngine) {
            return new RaftCountDownLatchService(nodeEngine);
        }
    }
}
