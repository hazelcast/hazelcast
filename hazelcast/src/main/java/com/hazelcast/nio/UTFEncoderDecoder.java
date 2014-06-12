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
import java.lang.reflect.Method;

public final class UTFEncoderDecoder {

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

    private static final UTFEncoderDecoder INSTANCE;

    static {
        INSTANCE = buildUTFUtil();
    }

    private final StringCreator stringCreator;
    private final boolean hazelcastEnterpriseActive;

    private UTFEncoderDecoder(boolean fastStringCreator) {
        this(fastStringCreator ? buildFastStringCreator() : new DefaultStringCreator(), false);
    }

    private UTFEncoderDecoder(StringCreator stringCreator, boolean hazelcastEnterpriseActive) {
        this.stringCreator = stringCreator;
        this.hazelcastEnterpriseActive = hazelcastEnterpriseActive;
    }

    public StringCreator getStringCreator() {
        return stringCreator;
    }

    public static void writeUTF(final DataOutput out, final String str, byte[] buffer) throws IOException {
        INSTANCE.writeUTF0(out, str, buffer);
    }

    public static String readUTF(final DataInput in, byte[] buffer) throws IOException {
        return INSTANCE.readUTF0(in, buffer);
    }

    public boolean isHazelcastEnterpriseActive() {
        return hazelcastEnterpriseActive;
    }

    public void writeUTF0(final DataOutput out, final String str, byte[] buffer) throws IOException {
        boolean isNull = str == null;
        out.writeBoolean(isNull);
        if (isNull) {
            return;
        }

        int length = str.length();
        out.writeInt(length);
        if (length > 0) {
            int chunkSize = (length / STRING_CHUNK_SIZE) + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
                writeShortUTF(out, str, beginIndex, endIndex, buffer);
            }
        }
    }

    private void writeShortUTF(final DataOutput out,
                               final String str,
                               final int beginIndex,
                               final int endIndex,
                               byte[] buffer) throws IOException {
        int utfLength = 0;
        int c = 0;
        int count = 0;
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
            if (!((c >= 0x0001) && (c <= 0x007F))) {
                break;
            }
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

    public String readUTF0(final DataInput in, byte[] buffer) throws IOException {
//        if (!isPowerOfTwo(buffer.length)) {
//            throw new IllegalArgumentException("Size of the buffer has to be power of two");
//        } //TODO: Do we want to be fast or defensive?
        boolean isNull = in.readBoolean();
        if (isNull) {
            return null;
        }
        int length = in.readInt();
        final char[] data = new char[length];
        if (length > 0) {
            int chunkSize = length / STRING_CHUNK_SIZE + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                readShortUTF(in, data, beginIndex, buffer);
            }
        }
        return stringCreator.buildString(data);
    }

    private boolean isPowerOfTwo(int x) {
        return (x & (x - 1)) == 0;
    }


    private void readShortUTF(final DataInput in, final char[] data,
                              final int beginIndex, byte[] buffer) throws IOException {
        final int utflen = in.readShort();
        int count = 0;
        int charArrCount = beginIndex;
        while (count < utflen) {
            int c = buffered(buffer, count++, utflen, in) & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    decodeOneByteChar(data, charArrCount, c);
                    break;
                case 12:
                case 13:
                    count = decodeTwoBytesChar(data, charArrCount, c, in, buffer, utflen, count);
                    break;
                case 14:
                    count = decodeThreeBytesChar(data, charArrCount, c, in, buffer, utflen, count);
                    break;
                default:
                    /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
            charArrCount++;
        }
    }

    private int decodeThreeBytesChar(char[] data, int charArrCount, int char1, DataInput in, byte[] buffer, int utflen, int count) throws IOException {
        /* 1110 xxxx 10xx xxxx 10xx xxxx */
        if (count + 2 > utflen) {
            throw new UTFDataFormatException("malformed input: partial character at end");
        }
        int char2 = buffered(buffer, count++, utflen, in);
        int char3 = buffered(buffer, count++, utflen, in);
        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
            throw new UTFDataFormatException("malformed input around byte " + (count - 1));
        }
        data[charArrCount] = (char) (((char1 & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
        return count;
    }

    private int decodeTwoBytesChar(char[] data, int charArrCount, int char1, DataInput in, byte[] buffer, int utflen, int count) throws IOException {
    /* 110x xxxx 10xx xxxx */
        if (count + 1 > utflen) {
            throw new UTFDataFormatException("malformed input: partial character at end");
        }
        int char2 = buffered(buffer, count++, utflen, in);
        if ((char2 & 0xC0) != 0x80) {
            throw new UTFDataFormatException("malformed input around byte " + count);
        }
        data[charArrCount] = (char) (((char1 & 0x1F) << 6) | (char2 & 0x3F));
        return count;
    }

    private void decodeOneByteChar(char[] data, int charArrCount, int c) {
    /* 0xxxxxxx */
        data[charArrCount] = (char) c;
        return;
    }

    private void buffering(byte[] buffer, int pos, byte value, DataOutput out) throws IOException {
        int innerPos = pos % buffer.length;
        if (pos > 0 && innerPos == 0) {
            out.write(buffer, 0, buffer.length);
        }
        buffer[innerPos] = value;
    }

    private byte buffered(byte[] buffer, int pos, int utfLength, DataInput in) throws IOException {
        int innerPos = pos & (buffer.length - 1); // it's the same as "pos % buffer.length" when buffer.length is power of two
        if (innerPos == 0) {
            int length = Math.min(buffer.length, utfLength - pos);
            in.readFully(buffer, 0, length);
        }
        return buffer[innerPos];
    }

    public static boolean useOldStringConstructor() {
        try {
            Class<String> clazz = String.class;
            clazz.getDeclaredConstructor(int.class, int.class, char[].class);
            return true;
        } catch (Throwable ignore) {
        }
        return false;
    }

    private static UTFEncoderDecoder buildUTFUtil() {
        try {
            Class<?> clazz = Class.forName("com.hazelcast.nio.utf8.EnterpriseStringCreator");
            Method method = clazz.getDeclaredMethod("findBestStringCreator");
            return new UTFEncoderDecoder((StringCreator) method.invoke(clazz), true);
        } catch (Throwable t) {
        }
        boolean faststringEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring", "true"));
        return new UTFEncoderDecoder(faststringEnabled ? buildFastStringCreator() : new DefaultStringCreator(), false);
    }

    private static StringCreator buildFastStringCreator() {
        try {
            // Give access to the package private String constructor
            Constructor<String> constructor = null;
            if (UTFEncoderDecoder.useOldStringConstructor()) {
                constructor = String.class.getDeclaredConstructor(int.class, int.class, char[].class);
            } else {
                constructor = String.class.getDeclaredConstructor(char[].class, boolean.class);
            }
            if (constructor != null) {
                constructor.setAccessible(true);
                return new FastStringCreator(constructor);
            }
        } catch (Throwable ignore) {
        }
        return null;
    }

    private static class DefaultStringCreator implements UTFEncoderDecoder.StringCreator {
        @Override
        public String buildString(char[] chars) {
            return new String(chars);
        }
    }

    private static class FastStringCreator implements UTFEncoderDecoder.StringCreator {

        private final Constructor<String> constructor;
        private final boolean useOldStringConstructor;

        public FastStringCreator(Constructor<String> constructor) {
            this.constructor = constructor;
            this.useOldStringConstructor = constructor.getParameterTypes().length == 3;
        }

        @Override
        public String buildString(char[] chars) {
            try {
                if (useOldStringConstructor) {
                    return constructor.newInstance(0, chars.length, chars);
                } else {
                    return constructor.newInstance(chars, Boolean.TRUE);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public interface StringCreator {
        String buildString(char[] chars);
    }

}
