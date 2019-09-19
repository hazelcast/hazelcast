package com.hazelcast.sql.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Descriptor of the fragment. Provides full information about fragment execution except of the fragment node itself
 * because the node is either serialized or not on per-member basis depending on whether the member will execute
 * the fragment or not.
 */
public class QueryFragmentDescriptor implements DataSerializable {
    /** Mapping type. */
    private QueryMapping mapping;

    /** Member IDs for custom mapping. */
    private List<String> customMappingMemberIds;

    /** Fragment parallelism. */
    private int parallelism;

    public QueryFragmentDescriptor() {
        // No-op.
    }

    public QueryFragmentDescriptor(QueryMapping mapping, List<String> customMappingMemberIds, int parallelism) {
        this.mapping = mapping;
        this.customMappingMemberIds = customMappingMemberIds;
        this.parallelism = parallelism;
    }

    public QueryMapping getMapping() {
        return mapping;
    }

    public List<String> getCustomMappingMemberIds() {
        return customMappingMemberIds != null ? customMappingMemberIds : Collections.emptyList();
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(mapping.getId());

        if (customMappingMemberIds == null) {
            out.writeInt(0);
        }
        else {
            out.writeInt(customMappingMemberIds.size());

            for (String memberId : customMappingMemberIds)
                out.writeUTF(memberId);
        }

        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapping = QueryMapping.getById(in.readInt());

        int customMappingMemberIdsSize = in.readInt();

        if (customMappingMemberIdsSize != 0) {
            customMappingMemberIds = new ArrayList<>(customMappingMemberIdsSize);

            for (int i = 0; i < customMappingMemberIdsSize; i++) {
                customMappingMemberIds.add(in.readUTF());
            }
        }

        parallelism = in.readInt();
    }
}
