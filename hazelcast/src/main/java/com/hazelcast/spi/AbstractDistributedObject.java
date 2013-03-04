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

package com.hazelcast.spi;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;

/**
 * @mdogan 1/14/13
 */
public abstract class AbstractDistributedObject<S extends RemoteService> implements DistributedObject {

    private volatile NodeEngine nodeEngine;
    private volatile S service;

    protected AbstractDistributedObject(NodeEngine nodeEngine, S service) {
        this.nodeEngine = nodeEngine;
        this.service = service;
    }

    public final void destroy() {
        final NodeEngine engine = getNodeEngine();
        engine.getProxyService().destroyDistributedObject(getServiceName(), getId());
    }

    public final NodeEngine getNodeEngine() {
        final NodeEngine engine = nodeEngine;
        lifecycleCheck(engine);
        return engine;
    }

    private void lifecycleCheck(final NodeEngine engine) {
        if (engine == null || !engine.isActive()) {
            throw throwNotActiveException();
        }
    }

    protected RuntimeException throwNotActiveException() {
        throw new HazelcastInstanceNotActiveException();
    }

    public final S getService() {
        final S s = service;
        if (s == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return s;
    }

    public abstract String getServiceName();

    final void invalidate() {
        nodeEngine = null;
        service = null;
    }
}
