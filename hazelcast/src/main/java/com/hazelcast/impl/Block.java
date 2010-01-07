/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

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
    private int hash = Integer.MIN_VALUE;

    public Block() {
    }

    public Block(Block block) {
        this(block.blockId, block.owner);
        migrationAddress = block.migrationAddress;
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
        hash = Integer.MIN_VALUE;
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

    public void readOwnership(DataInput in) throws IOException {
        this.blockId = in.readInt();
        boolean owned = in.readBoolean();
        if (owned) {
            this.owner = new Address();
            this.owner.readData(in);
        }
    }

    public void readData(DataInput in) throws IOException {
        readOwnership(in);
        boolean migrating = in.readBoolean();
        if (migrating) {
            setMigrationAddress(new Address());
            getMigrationAddress().readData(in);
        }
    }

    public void writeOwnership(DataOutput out) throws IOException {
        out.writeInt(this.blockId);
        boolean owned = (this.owner != null);
        out.writeBoolean(owned);
        if (owned) {
            this.owner.writeData(out);
        }
    }

    public void writeData(DataOutput out) throws IOException {
        writeOwnership(out);
        boolean migrating = (getMigrationAddress() != null);
        out.writeBoolean(migrating);
        if (migrating)
            getMigrationAddress().writeData(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Block block = (Block) o;
        if (blockId != block.blockId) return false;
        return true;
    }

    @Override
    public int hashCode() {
        if (hash == Integer.MIN_VALUE) {
            int result = blockId;
            result = 31 * result + (owner != null ? owner.hashCode() : 0);
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString() {
        return "Block [" + getBlockId() + "] owner=" + getOwner() + " migrationAddress="
                + getMigrationAddress();
    }
}
