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

package com.hazelcast.nio;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.junit.Test;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class SerializationTest {
    @Test
    public void testLongValueIncrement() {
        Data zero = toData(0L);
        Data five = IOUtil.addDelta(zero, 5L);
        assertEquals (5L, toObject(five));
        Data minusThree = IOUtil.addDelta(five, -8L);
        assertEquals (-3L, toObject(minusThree));
        Data minusTwo = IOUtil.addDelta(minusThree, 1L);
        assertEquals (-2L, toObject(minusTwo));
        Data twenty = IOUtil.addDelta(minusThree, 23L);
        assertEquals (20L, toObject(twenty));
    }
    
    @Test
    public void newSerializerExternalizable() {
    	final Serializer serializer = new Serializer();
        final ExternalizableImpl o = new ExternalizableImpl();
        o.s = "Gallaxy";
        o.v = 42;
        final Data data = serializer.writeObject(o);
		byte[] b = data.buffer;
        assertFalse(b.length == 0);
        assertFalse(o.readExternal);
        assertTrue(o.writeExternal);
        
        final ExternalizableImpl object = (ExternalizableImpl)serializer.readObject(data);
        assertNotNull(object);
        assertNotSame(o, object);
        assertEquals(o, object);
        assertTrue(object.readExternal);
        assertFalse(object.writeExternal);
    }
    
    
    public static class ExternalizableImpl implements Externalizable {
        private int v;
        private String s;
        
        private boolean readExternal = false;
        private boolean writeExternal = false;
        
        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof ExternalizableImpl)) return false;
            final ExternalizableImpl other = (ExternalizableImpl) obj;
            return this.v == other.v &&
                ((this.s == null && other.s == null) ||
                        (this.s != null && this.s.equals(other.s)));
        }
        
        @Override
        public int hashCode() {
            return this.v  + 31 * (s != null ? s.hashCode() : 0);
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            v = in.readInt();
            s = in.readUTF();
            readExternal = true;
        }
        
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(v);
            out.writeUTF(s);
            writeExternal = true;
        }
    }
}
