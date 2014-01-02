/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.nio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Constructor;
import java.io.*;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * @author mdogan 1/23/13
 */
public final class UTFUtil {

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

    /**
     * Converts a String to bytes. The String is allowed to be empty or null.
     *
     * @param str the String to convert.
     * @return the bytes.
     * @throws IOException if something fails during conversion.
     */
    public static byte[] toBytes(String str)throws IOException{
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOut);
        writeUTF(out,str);
        out.close();
        return byteArrayOut.toByteArray();
    }

    /**
     * Converts a byte array to a String. The returned String can be empty or null.
     *
     * @param bytes the bytes to convert to String.
     * @return the loaded String; could be null.
     * @throws IOException if something fails during conversion.
     * @throws java.lang.IllegalArgumentException if bytes is null.
     */
    public static String toString(byte[] bytes)throws IOException{
        isNotNull(bytes,"bytes");
        ByteArrayInputStream byteArrayIn = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(byteArrayIn);
        in.close();
        return readUTF(in);
    }

    private static final StringCreator STRING_CREATOR;

    static {
        boolean faststring = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring", "true"));
        StringCreator stringCreator = null;
        if (faststring) {
            try {
                Constructor<String> constructor = String.class.getDeclaredConstructor(char[].class, boolean.class);
                constructor.setAccessible(true);
                stringCreator = new FastStringCreator(constructor);
            } catch (Throwable t) {
                faststring = false;
            }
        }
        if (!faststring) {
            stringCreator = new DefaultStringCreator();
        }
        STRING_CREATOR = stringCreator;
    }

    public static void writeUTF(final DataOutput out, final String str, byte[] buffer) throws IOException {
        boolean isNull = str == null;
        out.writeBoolean(isNull);
        if (isNull) return;

        int length = str.length();
        out.writeInt(length);
        int chunkSize = (length / STRING_CHUNK_SIZE) + 1;
        for (int i = 0; i < chunkSize; i++) {
            int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
            int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
            writeShortUTF(out, str, beginIndex, endIndex, buffer);
        }
    }

    private static void writeShortUTF(final DataOutput out, final String str,
                                      final int beginIndex, final int endIndex,
                                      byte[] buffer) throws IOException {
        int utfLength = 0;
        int c, count = 0;
            /* use charAt instead of copying String to char array */
        for (int i = beginIndex; i < endIndex; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utfLength++;
            } else if (c > 0x07FF) {
                utfLength += 3;
            } else {
                utfLength += 2;
            }
        }
        if (utfLength > 65535) {
            throw new UTFDataFormatException("encoded string too long:"
                    + utfLength + " bytes");
        }
        out.writeShort(utfLength);
        int i;
        for (i = beginIndex; i < endIndex; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F)))
                break;
            buffering(buffer, count++, (byte) c, out);
        }
        for (; i < endIndex; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                buffering(buffer, count++, (byte) c, out);
            } else if (c > 0x07FF) {
                buffering(buffer, count++, (byte) (0xE0 | ((c >> 12) & 0x0F)), out);
                buffering(buffer, count++, (byte) (0x80 | ((c >> 6) & 0x3F)), out);
                buffering(buffer, count++, (byte) (0x80 | ((c) & 0x3F)), out);
            } else {
                buffering(buffer, count++, (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                buffering(buffer, count++, (byte) (0x80 | ((c) & 0x3F)), out);
            }
        }
        int length = count % buffer.length;
        out.write(buffer, 0, length == 0 ? buffer.length : length);
    }

    public static String readUTF(final DataInput in, byte[] buffer) throws IOException {
        boolean isNull = in.readBoolean();
        if (isNull) return null;
        int length = in.readInt();
        final char[] data = new char[length];
        int chunkSize = length / STRING_CHUNK_SIZE + 1;
        for (int i = 0; i < chunkSize; i++) {
            int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
            int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
            readShortUTF(in, data, beginIndex, endIndex, buffer);
        }
        return STRING_CREATOR.buildString(data);
    }

    private static void readShortUTF(final DataInput in, final char[] data,
                                       final int beginIndex, final int endIndex,
                                       byte[] buffer) throws IOException {
        final int utflen = in.readShort();
        int c = 0, char2, char3;
        int count = 0;
        int chararr_count = beginIndex;
        int lastCount = -1;
        while (count < utflen) {
            c = buffered(buffer, count, utflen, in) & 0xff;
            if (c > 127)
                break;
            lastCount = count;
            count++;
            data[chararr_count++] = (char) c;
        }
        while (count < utflen) {
            if (lastCount > -1 && lastCount < count) {
                c = buffered(buffer, count, utflen, in) & 0xff;
            }
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx */
                    lastCount = count;
                    count++;
                    data[chararr_count++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx 10xx xxxx */
                    lastCount = count++;
                    if (count + 1 > utflen)
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    char2 = buffered(buffer, count++, utflen, in);
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    data[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    lastCount = count++;
                    if (count + 2> utflen)
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    char2 = buffered(buffer, count++, utflen, in);
                    char3 = buffered(buffer, count++, utflen, in);
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException("malformed input around byte " + (count - 1));
                    data[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
    }

    private static void buffering(byte[] buffer, int pos, byte value, DataOutput out) throws IOException {
        int innerPos = pos % buffer.length;
        if (pos > 0 && innerPos == 0) {
            out.write(buffer, 0, buffer.length);
        }
        buffer[innerPos] = value;
    }

    private static byte buffered(byte[] buffer, int pos, int utfLenght, DataInput in) throws IOException {
        int innerPos = pos % buffer.length;
        if (innerPos == 0) {
            int length = Math.min(buffer.length, utfLenght - pos);
            in.readFully(buffer, 0, length);
        }
        return buffer[innerPos];
    }

    private static interface StringCreator {
        String buildString(char[] chars);
    }

    private static class DefaultStringCreator implements StringCreator {
        @Override
        public String buildString(char[] chars) {
            return new String(chars);
        }
    }

    private static class FastStringCreator implements StringCreator {

        private final Constructor<String> constructor;

        private FastStringCreator(Constructor<String> constructor) {
            this.constructor = constructor;
        }

        @Override
        public String buildString(char[] chars) {
            try {
                return constructor.newInstance(chars, Boolean.TRUE);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
