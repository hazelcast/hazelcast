/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;

/**
 * Convenient adapter for physical visitor which delegates all calls to a single method.
 */
public abstract class PhysicalRelVisitorAdapter implements PhysicalRelVisitor {
    @Override
    public void onRoot(RootPhysicalRel rel) {
        onNode(rel);
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        onNode(rel);
    }

    @Override
    public void onRootExchange(RootExchangePhysicalRel rel) {
        onNode(rel);
    }

    @Override
    public void onProject(ProjectPhysicalRel rel) {
        onNode(rel);
    }

    @Override
    public void onFilter(FilterPhysicalRel rel) {
        onNode(rel);
    }

    protected abstract void onNode(PhysicalRel rel);
}
