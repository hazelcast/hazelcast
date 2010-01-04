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

package com.hazelcast.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InitialProcess extends AbstractRemotelyProcessable {

    private static final long serialVersionUID = 4803635299019880729L;

    private List<AbstractRemotelyProcessable> lsProcessables = new ArrayList<AbstractRemotelyProcessable>(10);

    public List<AbstractRemotelyProcessable> getProcessables() {
        return lsProcessables;
    }

    public void writeData(DataOutput out) throws IOException {
        int size = lsProcessables.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            writeObject(out, lsProcessables.get(i));
        }
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            lsProcessables.add((AbstractRemotelyProcessable) readObject(in));
        }
    }

    public void process() {
        for (AbstractRemotelyProcessable processable : lsProcessables) {
            processable.setConnection(getConnection());
            processable.setNode(getNode());
            processable.process();
        }
    }
}
