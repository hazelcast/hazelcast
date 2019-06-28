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

package com.hazelcast.internal.query.physical;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.Predicate;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.physical.PhysicalNodeVisitor;

import java.io.IOException;
import java.util.List;

public class MapScanPhysicalNode implements PhysicalNode {

    private String mapName;
    private List<Expression> projections;
    private Predicate filter;
    private int parallelism;

    // TODO: Explicit partition list.

    public MapScanPhysicalNode() {
        // No-op.
    }

    public MapScanPhysicalNode(String mapName, List<Expression> projections, Predicate filter, int parallelism) {
        this.mapName = mapName;
        this.projections = projections;
        this.filter = filter;
        this.parallelism = parallelism;
    }

    public String getMapName() {
        return mapName;
    }

    public List<Expression> getProjections() {
        return projections;
    }

    public Predicate getFilter() {
        return filter;
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        visitor.onMapScanNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeObject(projections);
        out.writeObject(filter);
        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        projections = in.readObject();
        filter = in.readObject();
        parallelism = in.readInt();
    }
}
