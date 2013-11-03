package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        Config config = new Config();
        PortableFactory portalbeFactory = new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new Key();
            }
        };
        config.getSerializationConfig().addPortableFactory(1, portalbeFactory);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);

        IMap<Key, String> map = hz1.getMap("cache");

        Key key1 = new Key(1);
        Key key2 = new Key(2);
        Key key3 = new Key(3);

        keyCheck(hz1,key1);
        keyCheck(hz1,key2);
        keyCheck(hz1,key3);

        map.put(key1, "");
        map.put(key2, "");
        map.put(key3, "");

        map.remove(key1);
        map.remove(key2);
        map.remove(key3);
       System.out.println("size:" + map.size());
        System.out.println("done");
    }

    private static void keyCheck(HazelcastInstance hz, Key key) {
        Data data1 = HazelcastTestSupport.getNode(hz).getSerializationService().toData(key);
        for (int k = 0; k < 100; k++) {
            Data data2 = HazelcastTestSupport.getNode(hz).getSerializationService().toData(key);
            if (!data1.equals(data2)) {
                for(int l=0;l<data1.bufferSize();l++){
                    System.out.print(Integer.toHexString(data1.getBuffer()[l]));
                }
                System.out.println();
                for(int l=0;l<data2.bufferSize();l++){
                    System.out.print(Integer.toHexString(data2.getBuffer()[l]));
                }
                System.out.println();

                throw new RuntimeException();
            }
        }
    }
}

class Key implements Portable {
    protected long dataId;

    public Key() {
        super();
    }

    public Key(long dataId) {
        this.dataId = dataId;
    }

    public int getClassId() {
        return 1;
    }

    public int getFactoryId() {
        return 1;
    }

    public void readPortable(PortableReader in) throws IOException {
    //    dataId = in.readLong("dataId");
    }

    public void writePortable(PortableWriter out) throws IOException {
     //   out.writeLong("dataId", dataId);
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (dataId ^ (dataId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Key other = (Key) obj;
        if (dataId != other.dataId) {
            return false;
        }
        //    if (documentId != other.documentId) {
        //        return false;
        //    }
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    //@Override
    // public String toString() {
    //     return getClass().getSimpleName() + " [dataId=" + dataId + ", documentId=" + documentId + "]";
    // }

}