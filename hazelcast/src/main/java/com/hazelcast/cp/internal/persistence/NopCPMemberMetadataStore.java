package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;

public class NopCPMemberMetadataStore implements CPMemberMetadataStore {

    public static final CPMemberMetadataStore INSTANCE = new NopCPMemberMetadataStore();

    private NopCPMemberMetadataStore() {
    }

    @Override
    public boolean isMarkedAPMember() {
        return false;
    }

    @Override
    public boolean tryMarkAPMember() {
        return false;
    }

    @Override
    public void persistLocalMember(CPMember member) {
    }

    @Override
    public CPMember readLocalMember() {
        return null;
    }

}
