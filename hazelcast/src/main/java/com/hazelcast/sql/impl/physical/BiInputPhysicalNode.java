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

import java.io.IOException;

/**
 * A node having two inputs.
 */
public abstract class BiInputPhysicalNode implements PhysicalNode {
    /** Left input. */
    protected PhysicalNode left;

    /** Right input. */
    protected PhysicalNode right;

    protected BiInputPhysicalNode() {
        // No-op.
    }

    protected BiInputPhysicalNode(PhysicalNode left, PhysicalNode right) {
        this.left = left;
        this.right = right;
    }

    public PhysicalNode getLeft() {
        return left;
    }

    public PhysicalNode getRight() {
        return right;
    }

    @Override
    public final int getInputCount() {
        return 2;
    }

    @Override
    public final PhysicalNode getInput(int i) {
        if (i == 0) {
            return left;
        } else if (i == 1) {
            return right;
        }

        throw new IllegalArgumentException("Illegal input index: " + i);
    }

    @Override
    public final void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);

        writeData0(out);
    }

    @Override
    public final void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();

        readData0(in);
    }

    protected void writeData0(ObjectDataOutput out) throws IOException {
        // No-op.
    }

    protected void readData0(ObjectDataInput in) throws IOException {
        // No-op.
    }
}
