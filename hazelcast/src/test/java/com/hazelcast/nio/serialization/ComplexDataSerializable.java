package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
* @author mdogan 22/05/14
*/
class ComplexDataSerializable implements DataSerializable {

    private DataSerializable ds;
    private Portable portable;
    private DataSerializable ds2;

    ComplexDataSerializable() {
    }

    ComplexDataSerializable(Portable portable, DataSerializable ds, DataSerializable ds2) {
        this.portable = portable;
        this.ds = ds;
        this.ds2 = ds2;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(ds);
        out.writeObject(portable);
        out.writeObject(ds2);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        ds = in.readObject();
        portable = in.readObject();
        ds2 = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComplexDataSerializable that = (ComplexDataSerializable) o;

        if (ds != null ? !ds.equals(that.ds) : that.ds != null) return false;
        if (ds2 != null ? !ds2.equals(that.ds2) : that.ds2 != null) return false;
        if (portable != null ? !portable.equals(that.portable) : that.portable != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = ds != null ? ds.hashCode() : 0;
        result = 31 * result + (portable != null ? portable.hashCode() : 0);
        result = 31 * result + (ds2 != null ? ds2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ComplexDataSerializable{");
        sb.append("ds=").append(ds);
        sb.append(", portable=").append(portable);
        sb.append(", ds2=").append(ds2);
        sb.append('}');
        return sb.toString();
    }
}
