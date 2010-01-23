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

import com.hazelcast.cluster.AbstractRemotelyProcessable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Blocks extends AbstractRemotelyProcessable {
    List<Block> lsBlocks = new ArrayList<Block>(271);

    public void addBlock(Block block) {
        lsBlocks.add(block);
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Block block = new Block();
            block.readData(in);
            addBlock(block);
        }
    }

    public void writeData(DataOutput out) throws IOException {
        int size = lsBlocks.size();
        out.writeInt(size);
        for (Block block : lsBlocks) {
            block.writeData(out);
        }
    }

    public void process() {
        getNode().concurrentMapManager.partitionManager.handleBlocks(Blocks.this);
    }
}
