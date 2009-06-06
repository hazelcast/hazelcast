/**
 * 
 */
package com.hazelcast.impl.cluster;

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