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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Base class to scan a map.
 */
public abstract class AbstractMapScanPhysicalNode implements PhysicalNode {
    /** Map name. */
    private String mapName;

    /** Projections. */
    private List<Expression> projections;

    /** Filter. */
    private Expression<Boolean> filter;

    protected AbstractMapScanPhysicalNode() {
        // No-op.
    }

    protected AbstractMapScanPhysicalNode(String mapName, List<Expression> projections, Expression<Boolean> filter) {
        this.mapName = mapName;
        this.projections = projections;
        this.filter = filter;
    }

    public String getMapName() {
        return mapName;
    }

    public List<Expression> getProjections() {
        return projections;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeObject(projections);
        out.writeObject(filter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        projections = in.readObject();
        filter = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractMapScanPhysicalNode that = (AbstractMapScanPhysicalNode) o;

        return mapName.equals(that.mapName)
            && Objects.equals(projections, that.projections)
            && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapName, projections, filter);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", projections=" + projections
            + ", filter=" + filter + '}';
    }
}
