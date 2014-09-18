package com.hazelcast.nio.serialization;

import java.io.IOException;

/**
* @author mdogan 22/05/14
*/
class NamedPortableV2 extends NamedPortable implements VersionedPortable {

    private int v;

    NamedPortableV2() {
    }

    NamedPortableV2(int v) {
        this.v = v;
    }

    NamedPortableV2(String name, int v) {
        super(name, v * 10);
        this.v = v;
    }

    @Override
    public int getClassVersion() {
        return 2;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        writer.writeInt("v", v);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        v = reader.readInt("v");
    }

    public int getFactoryId() {
        return PortableTest.FACTORY_ID;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NamedPortableV2{");
        sb.append("name='").append(name).append('\'');
        sb.append(", k=").append(k);
        sb.append(", v=").append(v);
        sb.append('}');
        return sb.toString();
    }
}
