package com.hazelcast.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Block implements DataSerializable {
    private int blockId;
    private Address owner;
    private Address migrationAddress;

    public Block() {
    }

    public Block(int blockId, Address owner) {
        this.blockId = blockId;
        this.setOwner(owner);
    }

    public int getBlockId() {
        return blockId;
    }

    public Address getOwner() {
        return owner;
    }

    public void setOwner(Address owner) {
        this.owner = owner;
    }

    public Address getMigrationAddress() {
        return migrationAddress;
    }

    public void setMigrationAddress(Address migrationAddress) {
        this.migrationAddress = migrationAddress;
    }

    public boolean isMigrating() {
        return (getMigrationAddress() != null);
    }

    public Address getRealOwner() {
        return (getMigrationAddress() != null) ? getMigrationAddress() : getOwner();
    }

    public void readData(DataInput in) throws IOException {
        this.blockId = in.readInt();
        boolean owned = in.readBoolean();
        if (owned) {
            this.owner = new Address();
            this.owner.readData(in);
        }
        boolean migrating = in.readBoolean();
        if (migrating) {
            setMigrationAddress(new Address());
            getMigrationAddress().readData(in);
        }
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(getBlockId());
        boolean owned = (getOwner() != null);
        out.writeBoolean(owned);
        if (owned) {
            getOwner().writeData(out);
        }
        boolean migrating = (getMigrationAddress() != null);
        out.writeBoolean(migrating);
        if (migrating)
            getMigrationAddress().writeData(out);
    }

    @Override
    public String toString() {
        return "Block [" + getBlockId() + "] owner=" + getOwner() + " migrationAddress="
                + getMigrationAddress();
    }
}
