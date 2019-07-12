package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * Persists and restores CP member identity of the local member.
 */
public class CPMemberMetadataStore {

    private static final String CP_METADATA = "cp-metadata";


    private final File baseDir;

    public CPMemberMetadataStore(File baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * Returns true if this member is marked as AP member on the storage layer.
     * If {@code false} is returned, it means that AP/CP identity of the member
     * is not not known yet CP member discovery will run.
     */
    public boolean isMarkedAPMember() {
        File file = new File(baseDir, CP_METADATA);
        return file.exists() && file.length() == 0;
    }

    /**
     *  Marks this member as AP member on the storage layer.
     */
    public void markAPMember() throws IOException {
        File file = new File(baseDir, CP_METADATA);
        boolean created = file.createNewFile();
        assert created;
    }

    /**
     * Persists {@link CPMember} identity of the local member to storage.
     */
    public void persistLocalMember(CPMember member) throws IOException {
        File tmp = new File(baseDir, CP_METADATA + ".tmp");
        FileOutputStream fileOutputStream = new FileOutputStream(tmp);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
        try {
            out.writeUTF(member.getUuid());
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            IOUtil.closeResource(fileOutputStream);
            IOUtil.closeResource(out);
        }
        IOUtil.rename(tmp, new File(baseDir, "cp-metadata"));
    }

    /**
     * Reads {@link CPMember} identity of this member from storage.
     * If {@code null} is returned, it means that AP/CP identity of the member
     * is not not known yet CP member discovery will run.
     */
    public CPMember readLocalMember(Address address) throws IOException {
        File file = new File(baseDir, CP_METADATA);
        if (!file.exists()) {
            return null;
        }
        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        try {
            String uuid = in.readUTF();
            return new CPMemberInfo(UUID.fromString(uuid), address);
        } finally {
            IOUtil.closeResource(in);
        }
    }
}
