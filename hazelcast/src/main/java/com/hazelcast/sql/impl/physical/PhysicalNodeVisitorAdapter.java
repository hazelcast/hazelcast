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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.internal.query.physical.MapScanPhysicalNode;
import com.hazelcast.internal.query.physical.ReceivePhysicalNode;
import com.hazelcast.internal.query.physical.ReceiveSortMergePhysicalNode;
import com.hazelcast.internal.query.physical.RootPhysicalNode;
import com.hazelcast.internal.query.physical.SendPhysicalNode;
import com.hazelcast.internal.query.physical.SortPhysicalNode;

/**
 * Node visitor adapter.
 */
public abstract class PhysicalNodeVisitorAdapter implements PhysicalNodeVisitor {
    @Override
    public void onRootNode(RootPhysicalNode node) {
        // No-op.
    }

    @Override
    public void onReceiveNode(ReceivePhysicalNode node) {
        // No-op.
    }

    @Override
    public void onSendNode(SendPhysicalNode node) {
        // No-op.
    }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        // No-op.
    }

    @Override
    public void onSortNode(SortPhysicalNode node) {
        // No-op.
    }

    @Override
    public void onReceiveSortMergeNode(ReceiveSortMergePhysicalNode node) {
        // No-op.
    }
}
