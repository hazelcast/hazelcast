/**
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