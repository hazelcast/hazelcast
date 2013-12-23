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

/**
 * @author mdogan 1/23/13
 */
public final class UTFUtil {

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

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

    public static void writeUTF(final DataOutput out, final String str) throws IOException {
        boolean isNull = str == null;
        out.writeBoolean(isNull);
        if (isNull) return;

        int length = str.length();
        out.writeInt(length);
        int chunkSize = (length / STRING_CHUNK_SIZE) + 1;
        for (int i = 0; i < chunkSize; i++) {
            int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
            int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
            writeShortUTF(out, str, beginIndex, endIndex);
        }
    }

    private static void writeShortUTF(final DataOutput out, final String str,
                                      final int beginIndex, final int endIndex) throws IOException {
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
        final byte[] byteArray = new byte[utfLength];
        for (i = beginIndex; i < endIndex; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F)))
                break;
            byteArray[count++] = (byte) c;
        }
        for (; i < endIndex; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                byteArray[count++] = (byte) c;
            } else if (c > 0x07FF) {
                byteArray[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                byteArray[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                byteArray[count++] = (byte) (0x80 | ((c) & 0x3F));
            } else {
                byteArray[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                byteArray[count++] = (byte) (0x80 | ((c) & 0x3F));
            }
        }
        out.write(byteArray, 0, utfLength);
    }

    public static String readUTF(final DataInput in) throws IOException {
        boolean isNull = in.readBoolean();
        if (isNull) return null;
        int length = in.readInt();
        final char[] data = new char[length];
        int chunkSize = length / STRING_CHUNK_SIZE + 1;
        for (int i = 0; i < chunkSize; i++) {
            int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
            int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
            readShortUTF(in, data, beginIndex, endIndex);
        }
        return STRING_CREATOR.buildString(data);
    }

    private static void readShortUTF(final DataInput in, final char[] data,
                                       final int beginIndex, final int endIndex) throws IOException {
        final int utflen = in.readShort();
        byte[] bytearr = null;
        bytearr = new byte[utflen];
        int c, char2, char3;
        int count = 0;
        int chararr_count = beginIndex;
        in.readFully(bytearr, 0, utflen);
        while (count < utflen) {
            c = bytearr[count] & 0xff;
            if (c > 127)
                break;
            count++;
            data[chararr_count++] = (char) c;
        }
        while (count < utflen) {
            c = bytearr[count] & 0xff;
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
                    count++;
                    data[chararr_count++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx 10xx xxxx */
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    char2 = bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    data[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    char2 = bytearr[count - 2];
                    char3 = bytearr[count - 1];
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
                return constructor.newInstance(chars, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
