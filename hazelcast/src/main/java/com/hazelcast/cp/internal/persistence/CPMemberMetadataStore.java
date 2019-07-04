package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.nio.Address;

import java.io.IOException;

/**
 * Persists and restores CP member identity of the local member.
 */
public interface CPMemberMetadataStore {

    /**
     * Returns true if this member is marked as AP member on the storage layer.
     * If {@code false} is returned, it means that AP/CP identity of the member
     * is not not known yet CP member discovery will run.
     */
    boolean isMarkedAPMember();

    /**
     *  Marks this member as AP member on the storage layer,
     *  if it is not a CP member already.
     * @return true if marked as AP, false otherwise
     */
    boolean tryMarkAPMember() throws IOException;

    /**
     * Persists {@link CPMember} identity of the local member to storage.
     */
    void persistLocalMember(CPMember member) throws IOException;

    /**
     * Reads {@link CPMember} identity of this member from storage.
     * If {@code null} is returned, it means that AP/CP identity of the member
     * is not not known yet CP member discovery will run.
     */
    CPMember readLocalMember() throws IOException;

}
