package com.hazelcast.nio.serialization;

import java.io.IOException;

/**
* @author mdogan 22/05/14
*/
class NamedPortable implements Portable {
    static final int CLASS_ID = 3;

    String name;
    int k;

    NamedPortable() {
    }

    NamedPortable(String name, int k) {
        this.name = name;
        this.k = k;
    }

    public int getClassId() {
        return CLASS_ID;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
        writer.writeInt("myint", k);
    }

    public void readPortable(PortableReader reader) throws IOException {
        k = reader.readInt("myint");
        name = reader.readUTF("name");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NamedPortable that = (NamedPortable) o;

        if (k != that.k) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + k;
        return result;
    }

    public int getFactoryId() {
        return PortableTest.FACTORY_ID;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NamedPortable{");
        sb.append("name='").append(name).append('\'');
        sb.append(", k=").append(k);
        sb.append('}');
        return sb.toString();
    }
}
