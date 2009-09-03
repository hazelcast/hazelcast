package com.hazelcast.nio;

import java.io.*;

public class SerializationHelper {
    protected void writeObject(DataOutput out, Object obj) throws IOException {
//            if (value instanceof Long) {
//                out.writeByte(1);
//                out.writeLong((Long) value);
//            } else if (value instanceof Integer) {
//                out.writeByte(2);
//                out.writeInt((Integer) value);
//            } else if (value instanceof String) {
//                out.writeByte(3);
//                out.writeUTF((String) value);
//            } else if (value instanceof Double) {
//                out.writeByte(4);
//                out.writeDouble((Double) value);
//            } else if (value instanceof Float) {
//                out.writeByte(5);
//                out.writeDouble((Float) value);
//            } else if (value instanceof Boolean) {
//                out.writeByte(6);
//                out.writeBoolean((Boolean) value);
//            } else {
//                out.writeByte(7);
//                ObjectOutputStream os = new ObjectOutputStream((DataOutputStream) out);
//                os.writeObject(value);
//            }

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            byte[] buf = bos.toByteArray();
            out.writeInt(buf.length);
            out.write(buf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected Object readObject(DataInput in) throws IOException {
        try {
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readFully(buf);
            ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(buf));
            Object result = oin.readObject();
            oin.close();
            return result;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
//            byte type = in.readByte();
//            if (type == 1) {
//                return in.readLong();
//            } else if (type == 2) {
//                return in.readUTF();
//            } else if (type == 3) {
//                return in.readBoolean();
//            } else {
//                throw new IOException("Unknown object type=" + type);
//            }
    }
}
