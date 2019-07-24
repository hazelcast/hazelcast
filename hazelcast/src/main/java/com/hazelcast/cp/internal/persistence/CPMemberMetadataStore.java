package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;

import java.io.IOException;

public interface CPMemberMetadataStore {

    boolean isMarkedAPMember();

    void markAPMember() throws IOException;

    void persistLocalMember(CPMember member) throws IOException;

    CPMember readLocalMember() throws IOException;

}
