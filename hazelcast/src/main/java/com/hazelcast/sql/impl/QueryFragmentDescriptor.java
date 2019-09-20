package com.hazelcast.sql.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.physical.PhysicalNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Query fragment descriptor which is sent over wire.
 */
public class QueryFragmentDescriptor implements DataSerializable {
    /** Physical node. */
    private PhysicalNode node;

    /** Mapping. */
    private QueryFragmentMapping mapping;

    /** IDs of mapped nodes. May be null if nodes could be inherited from the context. */
    private List<String> mappedMemberIds;

    /**
     * Offset of the first stripe of this fragment. We use it to determine the data thread where the
     * fragment deployments are going to be executed.
     */
    private int deploymentOffset;

    public QueryFragmentDescriptor() {
        // No-op.
    }

    public QueryFragmentDescriptor(
        PhysicalNode node,
        QueryFragmentMapping mapping,
        List<String> mappedMemberIds,
        int deploymentOffset
    ) {
        this.node = node;
        this.mapping = mapping;
        this.mappedMemberIds = mappedMemberIds;
        this.deploymentOffset = deploymentOffset;
    }

    public PhysicalNode getNode() {
        return node;
    }

    public QueryFragmentMapping getMapping() {
        return mapping;
    }

    public List<String> getMappedMemberIds() {
        return mappedMemberIds != null ? mappedMemberIds : Collections.emptyList();
    }

    public int getDeploymentOffset() {
        return deploymentOffset;
    }

    public int getAbsoluteDeploymentOffset(QueryExecuteOperation operation) {
        return operation.getBaseDeploymentOffset() + deploymentOffset;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(node);
        out.writeInt(mapping.getId());

        List<String> mappedMemberIds0 = getMappedMemberIds();

        out.writeInt(mappedMemberIds0.size());

        for (String mappedMemberId : mappedMemberIds0) {
            out.writeUTF(mappedMemberId);
        }

        out.writeInt(deploymentOffset);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        node = in.readObject();
        mapping = QueryFragmentMapping.getById(in.readInt());

        int mappedMemberIdsSize = in.readInt();

        if (mappedMemberIdsSize > 0) {
            mappedMemberIds = new ArrayList<>(mappedMemberIdsSize);

            for (int i = 0; i < mappedMemberIdsSize; i++) {
                mappedMemberIds.add(in.readUTF());
            }
        }

        deploymentOffset = in.readInt();
    }

    /**
     * Get members participating in the given fragment.
     *
     * @param dataMemberIds Data member IDs.
     * @return Members participating in the given fragment.
     */
    public Collection<String> getFragmentMembers(Collection<String> dataMemberIds) {
        if (mapping == QueryFragmentMapping.ROOT || mapping == QueryFragmentMapping.REPLICATED) {
            return getMappedMemberIds();
        }
        else {
            assert mapping == QueryFragmentMapping.DATA_MEMBERS;

            return dataMemberIds;
        }
    }
}
