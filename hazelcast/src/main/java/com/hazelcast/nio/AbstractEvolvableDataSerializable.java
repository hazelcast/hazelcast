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
/*
   contributed by Eugen Duck <eugen@dueck.org>
*/
package com.hazelcast.nio;

import java.io.*;

public abstract class AbstractEvolvableDataSerializable implements
        DataSerializable, Evolvable {

    private byte[] futureData;

    public byte[] getFutureData() {
        return futureData; // offensively not-copying it, assuming we live in
//a world where no receiver is going to modify it;
    }

    public final void writeData(DataOutput out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        writeKnownData(dos);
        dos.close();
        baos.close();
        byte[] knownData = baos.toByteArray();
        int futureSize = futureData == null ? 0 : futureData.length;
        out.writeInt(knownData.length + futureSize);
        out.write(knownData);
        if (futureData != null)
            out.write(futureData);
    }

    public final void readData(DataInput in) throws IOException {
        int totalLength = in.readInt();
        byte[] allData = new byte[totalLength];
        in.readFully(allData);
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(allData);
            DataInputStream dis = new DataInputStream(bais);
            readKnownData(dis);
            futureData = new byte[dis.available()];
            dis.readFully(futureData);
        } catch (EOFException ignored) {
        }
    }

    /**
     * Serializes all data the version of this class at hand knows
     * about. AbstractEvolvableDataSerializable takes care
     * of putting potential unknown data (it deserialized) back on the
     * wire so that it does not get lost.
     */
    public abstract void writeKnownData(DataOutput out) throws
            IOException;

    /**
     * Deserializes all data the version of this class at hand knows
     * about. AbstractEvolvableDataSerializable takes care
     * of receiving and storing potential unknown data from the wire
     * in case a version newer than the one at hand was received.
     */
    public abstract void readKnownData(DataInput in) throws
            IOException;
}
