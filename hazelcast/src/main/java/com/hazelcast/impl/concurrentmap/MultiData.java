/**
 * 
 */
package com.hazelcast.impl.concurrentmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

public class MultiData implements DataSerializable {
    List<Data> lsData = null;

    public MultiData() {
    }

    public MultiData(Data d1, Data d2) {
        lsData = new ArrayList<Data>(2);
        lsData.add(d1);
        lsData.add(d2);
    }

    public void writeData(DataOutput out) throws IOException {
        int size = lsData.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            Data d = lsData.get(i);
            d.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        int size = in.readInt();
        lsData = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            lsData.add(data);
        }
    }

    public int size() {
        return (lsData == null) ? 0 : lsData.size();
    }

    public List<Data> getAllData() {
        return lsData;
    }

    public Data getData(int index) {
        return (lsData == null) ? null : lsData.get(index);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MultiData");
        sb.append("{size=").append(size());
        sb.append('}');
        return sb.toString();
    }
}