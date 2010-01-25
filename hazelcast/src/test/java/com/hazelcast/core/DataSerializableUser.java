/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
package com.hazelcast.core;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataSerializableUser implements DataSerializable{
    private String name = "";
    private String familyName = "";
    private int age;
    private Address address = new Address();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public void writeData(DataOutput out) throws IOException {

//        out.writeInt(name.getBytes().length);
//        out.write(name.getBytes());
        out.writeUTF(name);
//        out.writeInt(familyName.getBytes().length);
//        out.write(familyName.getBytes());
        out.writeUTF(familyName);
        out.writeInt(age);
        address.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
//        int lName = in.readInt();
//        byte[] bName = new byte[lName];
//        in.readFully(bName);
//        name = new String(bName);
//        int lFamily = in.readInt();
//        byte[] bFamily = new byte[lFamily];
//        in.readFully(bFamily);
//        familyName = new String(bFamily);
        name = in.readUTF();
        familyName = in.readUTF();
        age = in.readInt();
        address = new Address();
        address.readData(in);
    }

    public static class Address implements DataSerializable{
        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        private String address = "";
        public void writeData(DataOutput out) throws IOException {
//            out.writeInt(address.getBytes().length);
//            out.write(address.getBytes());
            out.writeUTF(address);
        }

        public void readData(DataInput in) throws IOException {
//            int len = in.readInt();
//            byte[] b = new byte[len];
//            in.readFully(b);
//            address = new String(b);
            address = in.readUTF();
        }
    }
}
