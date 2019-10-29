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

package com.hazelcast.sql.impl.calcite.cost.metadata;

import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public final class RowCountMetadata extends RelMdRowCount {
    /** Provider to be registered in Calcite. */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, new RowCountMetadata());

    private RowCountMetadata() {
        // No-op.
    }

    @Override
    public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
        HazelcastTable table = rel.getTable().unwrap(HazelcastTable.class);

        double count;

        if (table.hasContainer()) {
            Object container = table.getContainer();

            // TODO: Cache this!
            if (table.isPartitioned()) {
                count = ((MapProxyImpl) container).size();
            } else {
                count = ((ReplicatedMapProxy) container).size();
            }
        } else {
            count = super.getRowCount(rel, mq);
        }

        return count;
    }
}
