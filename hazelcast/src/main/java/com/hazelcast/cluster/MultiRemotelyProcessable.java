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
import java.util.LinkedList;
import java.util.List;

public class MultiRemotelyProcessable extends AbstractRemotelyProcessable {
    List<RemotelyProcessable> lsProcessables = new LinkedList<RemotelyProcessable>();

    public void add(RemotelyProcessable rp) {
        if (rp != null) {
            lsProcessables.add(rp);
        }
    }

    @Override
    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String className = in.readUTF();
            try {
                RemotelyProcessable rp = (RemotelyProcessable) Class.forName(className)
                        .newInstance();
                rp.readData(in);
                lsProcessables.add(rp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        out.writeInt(lsProcessables.size());
        for (RemotelyProcessable remotelyProcessable : lsProcessables) {
            out.writeUTF(remotelyProcessable.getClass().getName());
            remotelyProcessable.writeData(out);
        }
    }

    public void process() {
        for (RemotelyProcessable remotelyProcessable : lsProcessables) {
            remotelyProcessable.process();
        }
    }
}
