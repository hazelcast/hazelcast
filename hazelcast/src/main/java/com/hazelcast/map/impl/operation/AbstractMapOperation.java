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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.AbstractNamedOperation;

public abstract class AbstractMapOperation extends AbstractNamedOperation {

    protected transient MapService mapService;
    protected transient MapContainer mapContainer;

    public AbstractMapOperation() {
    }

    public AbstractMapOperation(String name) {
        super();
        this.name = name;
    }

    @Override
    public final void beforeRun() throws Exception {
        mapService = getService();
        mapContainer = mapService.getMapServiceContext().getMapContainer(name);
        innerBeforeRun();
    }

    public void innerBeforeRun() {
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

}
