package com.hazelcast.internal.query.physical;

import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.expression.Predicate;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

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
