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

import com.hazelcast.logging.Logger;
import com.hazelcast.util.QuickMath;

import sun.misc.Unsafe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Class to encode/decode UTF-Strings to and from byte-arrays.
 */
public final class UTFEncoderDecoder {

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

    private static final UTFEncoderDecoder INSTANCE;
    private static final long STRING_VALUE_FIELD_OFFSET;
    private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    private static final DefaultDataOutputUtfWriter DEFAULT_DATA_OUTPUT_UTF_WRITER =
            new DefaultDataOutputUtfWriter();
    private static final BufferedDataOutputUtfWriter BUFFERED_DATA_OUTPUT_UTF_WRITER =
            new BufferedDataOutputUtfWriter();

    static {
        INSTANCE = buildUTFUtil();
        try {
            STRING_VALUE_FIELD_OFFSET = UNSAFE.objectFieldOffset(String.class
                    .getDeclaredField("value"));
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Unable to get value field offset in String class", e);
        }
    }

    private final StringCreator stringCreator;
    private final boolean hazelcastEnterpriseActive;

    private UTFEncoderDecoder(boolean fastStringCreator) {
        this(fastStringCreator ? buildFastStringCreator() : new DefaultStringCreator(), false);
    }

    private UTFEncoderDecoder(StringCreator stringCreator,
                                       boolean hazelcastEnterpriseActive) {
        this.stringCreator = stringCreator;
        this.hazelcastEnterpriseActive = hazelcastEnterpriseActive;
    }

    public StringCreator getStringCreator() {
        return stringCreator;
    }

    public static void writeUTF(final DataOutput out,
                                final String str,
                                final byte[] buffer) throws IOException {
        INSTANCE.writeUTF0(out, str, buffer);
    }

    public static String readUTF(   final DataInput in,
                                    final byte[] buffer) throws IOException {
        return INSTANCE.readUTF0(in, buffer);
    }

    public boolean isHazelcastEnterpriseActive() {
        return hazelcastEnterpriseActive;
    }

    public void writeUTF0(  final DataOutput out,
                            final String str,
                            final byte[] buffer) throws IOException {
        if (!QuickMath.isPowerOfTwo(buffer.length)) {
            throw new IllegalArgumentException(
                    "Size of the buffer has to be power of two, was " + buffer.length);
        }
        boolean isNull = str == null;
        out.writeBoolean(isNull);
        if (isNull) {
            return;
        }

        final DataOutputAwareUtfWriter UTF_WRITER =
                out instanceof BufferObjectDataOutput
                        ? BUFFERED_DATA_OUTPUT_UTF_WRITER
                        : DEFAULT_DATA_OUTPUT_UTF_WRITER;
        int length = str.length();
        out.writeInt(length);
        out.writeInt(length);
        if (length > 0) {
            int chunkSize = (length / STRING_CHUNK_SIZE) + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
                UTF_WRITER.writeShortUTF(out, str, beginIndex, endIndex, buffer);
            }
        }
    }

    private interface DataOutputAwareUtfWriter {

        void writeShortUTF( final DataOutput out,
                            final String str,
                            final int beginIndex,
                            final int endIndex,
                            final byte[] buffer) throws IOException;

    }

    private static class BufferedDataOutputUtfWriter implements DataOutputAwareUtfWriter {

        @Override
        public void writeShortUTF(  final DataOutput out,
                                    final String str,
                                    final int beginIndex,
                                    final int endIndex,
                                    final byte[] buffer) throws IOException {
            char[] chars = (char[]) UNSAFE.getObject(str, STRING_VALUE_FIELD_OFFSET);
            BufferObjectDataOutput bufferObjectDataOutput = (BufferObjectDataOutput) out;

            int i;
            int c;
            int bufferPos = 0;
            int utfLength = 0;
            int maxUtfLength = chars.length * 3; // At most, one character can hold 3 bytes

            // We save current position of buffer data output.
            // Then we write the length of UTF to here
            final int pos = bufferObjectDataOutput.position();
            bufferObjectDataOutput.position(pos + 2);

            if (buffer.length >= maxUtfLength) {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) { // X == 0 or 0x007F < X < 0x7FFF
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else if (c > 0x007F) { // 0x007F < X <= 0x7FFF
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else { // 0x0001 <= X <= 0x007F
                        buffer[bufferPos++] = (byte) c;
                    }
                }
                utfLength = bufferPos;
                out.write(buffer, 0, bufferPos);
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                    utfLength++;
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) { // X == 0 or 0x007F < X < 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        utfLength += 2;
                    } else if (c > 0x007F) { // 0x007F < X <= 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xE0 | ((c >> 12) & 0x0F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c >> 6) & 0x3F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        utfLength += 3;
                    } else { // 0x0001 <= X <= 0x007F
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                        utfLength++;
                    }
                }
                int length = bufferPos % buffer.length;
                out.write(buffer, 0, length == 0 ? buffer.length : length);
            }

            if (utfLength > 65535) {
                throw new UTFDataFormatException(
                        "encoded string too long:" + utfLength + " bytes");
            }

            // Write the length of UTF to save position before
            bufferObjectDataOutput.writeShort(pos, utfLength);
        }

    }

    private static class DefaultDataOutputUtfWriter implements DataOutputAwareUtfWriter {

        @Override
        public void writeShortUTF(  final DataOutput out,
                                    final String str,
                                    final int beginIndex,
                                    final int endIndex,
                                    final byte[] buffer) throws IOException {
            char[] chars = (char[]) UNSAFE.getObject(str, STRING_VALUE_FIELD_OFFSET);

            int utfLength = calculateUtf8Length(chars, beginIndex, endIndex);
            if (utfLength > 65535) {
                throw new UTFDataFormatException(
                        "encoded string too long:" + utfLength + " bytes");
            }

            out.writeShort(utfLength);

            int i;
            int c;
            int bufferPos = 0;

            if (utfLength >= buffer.length) {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) { // X == 0 or 0x007F < X < 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                    } else if (c > 0x007F) { // 0x007F < X <= 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xE0 | ((c >> 12) & 0x0F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c >> 6) & 0x3F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                    } else { // 0x0001 <= X <= 0x007F
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                    }
                }
                int length = bufferPos % buffer.length;
                out.write(buffer, 0, length == 0 ? buffer.length : length);
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = chars[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }
                for (; i < endIndex; i++) {
                    c = chars[i];
                    if (c <= 0) { // X == 0 or 0x007F < X < 0x7FFF
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else if (c > 0x007F) { // 0x007F < X <= 0x7FFF
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else { // 0x0001 <= X <= 0x007F
                        buffer[bufferPos++] = (byte) c;
                    }
                }
                out.write(buffer, 0, bufferPos);
            }
        }

    }

    public String readUTF0(final DataInput in, final byte[] buffer) throws IOException {
        if (!QuickMath.isPowerOfTwo(buffer.length)) {
            throw new IllegalArgumentException(
                    "Size of the buffer has to be power of two, was " + buffer.length);
        }
        boolean isNull = in.readBoolean();
        if (isNull) {
            return null;
        }
        int length = in.readInt();
        int lengthCheck = in.readInt();
        if (length != lengthCheck) {
            throw new UTFDataFormatException(
                    "Length check failed, maybe broken bytestream or wrong stream position");
        }
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

    private void readShortUTF(  final DataInput in,
                                final char[] data,
                                final int beginIndex,
                                final byte[] buffer) throws IOException {
        final int utfLength = in.readShort();
        // buffer[0] is used to hold read data
        // so actual useful length of buffer is as "length - 1"
        final int minUtfLenght = Math.min(utfLength, buffer.length - 1);
        int readCount = 0;
        int bufferPos = 1; // We use buffer[0] to hold read data, so position starts from 1
        int i = 0;
        int c1 = 0, c2 = 0, c3 = 0, cTemp = 0;
        int charArrCount = beginIndex;

        // The first readable data is at 1. index since 0. index is used to hold read data.
        in.readFully(buffer, 1, minUtfLenght);

        for (; i < minUtfLenght; i++) {
            if ((c1 = buffer[bufferPos++] & 0xFF) > 127) {
                break;
            }
            data[charArrCount++] = (char) c1;
        }

        for (; i < utfLength; i++) {
            if (c1 > 127) {
                break;
            }
            data[charArrCount++] = (char) c1;
            bufferPos = buffered(buffer, bufferPos, utfLength, in);
            c1 = buffer[0] & 0xFF;
        }

        for (readCount = i; readCount < utfLength;) {
            cTemp = c1 >> 4;
            if (cTemp >> 3 == 0) { // ((cTemp & 0xF8) == 0) or (cTemp <= 7 && cTemp >= 0)
                /* 0xxxxxxx */
                data[charArrCount++] = (char) c1;
                readCount++;
            } else if (cTemp == 12 || cTemp == 13) {
                /* 110x xxxx 10xx xxxx */
                if (readCount + 1 > utfLength) {
                    throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                }
                bufferPos = buffered(buffer, bufferPos, utfLength, in);
                c2 = buffer[0] & 0xFF;
                if ((c2 & 0xC0) != 0x80) {
                    throw new UTFDataFormatException(
                            "malformed input around byte " + beginIndex + readCount + 1);
                }
                data[charArrCount++] = (char) (((c1 & 0x1F) << 6) | (c2 & 0x3F));
                readCount += 2;
            } else if (cTemp == 14) {
                /* 1110 xxxx 10xx xxxx 10xx xxxx */
                if (readCount + 2 > utfLength) {
                    throw new UTFDataFormatException(
                            "malformed input: partial character at end");
                }
                bufferPos = buffered(buffer, bufferPos, utfLength, in);
                c2 = buffer[0] & 0xFF;
                bufferPos = buffered(buffer, bufferPos, utfLength, in);
                c3 = buffer[0] & 0xFF;
                if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
                    throw new UTFDataFormatException(
                            "malformed input around byte " + (beginIndex + readCount + 1));
                }
                data[charArrCount++] = (char) (((c1 & 0x0F) << 12)
                        | ((c2 & 0x3F) << 6) | ((c3 & 0x3F)));
                readCount += 3;
            } else {
                /* 10xx xxxx, 1111 xxxx */
                throw new UTFDataFormatException(
                        "malformed input around byte " + (beginIndex + readCount));
            }

            bufferPos = buffered(buffer, bufferPos, utfLength, in);
            c1 = buffer[0] & 0xFF;
        }
    }

    private static int calculateUtf8Length( final char[] chars,
                                            final int beginIndex,
                                            final int endIndex) {
        int utfLength = 0;
        for (int i = beginIndex; i < endIndex; i++) {
            int c = chars[i];
            if (c <= 0) { // X == 0 or 0x007F < X < 0x7FFF
                utfLength += 2;
            } else if (c > 0x007F) { // 0x007F < X <= 0x7FFF
                utfLength += 3;
            } else { // 0x0001 <= X <= 0x007F
                utfLength++;
            }
        }
        return utfLength;
    }

    private static int buffering(   final byte[] buffer,
                                    final int pos,
                                    final byte value,
                                    final DataOutput out) throws IOException {
        try {
            buffer[pos] = value;
            return pos + 1;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Array bounds check by programmatically is not needed like
            // "if (pos < buffer.length)".
            // JVM checks instead of us, so it is unnecessary.
            out.write(buffer, 0, buffer.length);
            buffer[0] = value;
            return 1;
        }
    }

    private int buffered(   final byte[] buffer,
                            final int pos,
                            final int utfLength,
                            final DataInput in) throws IOException {
        try {
            // 0. index of buffer is used to hold read data
            // so copy read data to there.
            buffer[0] = buffer[pos];
            return pos + 1;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Array bounds check by programmatically is not needed like
            // "if (pos < buffer.length)".
            // JVM checks instead of us, so it is unnecessary.
            in.readFully(buffer, 1,
                    Math.min(buffer.length - 1, utfLength - pos));
            // The first readable data is at 1. index since 0. index is used to
            // hold read data.
            // So the next one will be 2. index.
            buffer[0] = buffer[1];
            return 2;
        }
    }

    public static boolean useOldStringConstructor() {
        try {
            Class<String> clazz = String.class;
            clazz.getDeclaredConstructor(int.class, int.class, char[].class);
            return true;
        } catch (Throwable t) {
            Logger.
                    getLogger(UTFEncoderDecoder.class).
                    finest("Old String constructor doesn't seem available", t);
        }
        return false;
    }

    private static UTFEncoderDecoder buildUTFUtil() {
        try {
            Class<?> clazz =
                    Class.forName("com.hazelcast.nio.utf8.EnterpriseStringCreator");
            Method method = clazz.getDeclaredMethod("findBestStringCreator");
            return new UTFEncoderDecoder(
                    (StringCreator) method.invoke(clazz), true);
        } catch (Throwable t) {
            Logger.
                    getLogger(UTFEncoderDecoder.class).
                    finest("EnterpriseStringCreator not available on classpath", t);
        }
        boolean faststringEnabled =
                Boolean.parseBoolean(
                        System.getProperty("hazelcast.nio.faststring", "true"));
        return new UTFEncoderDecoder(
                faststringEnabled
                        ? buildFastStringCreator()
                        : new DefaultStringCreator(), false);
    }

    private static StringCreator buildFastStringCreator() {
        try {
            // Give access to the package private String constructor
            Constructor<String> constructor = null;
            if (UTFEncoderDecoder.useOldStringConstructor()) {
                constructor =
                        String.class.getDeclaredConstructor(int.class, int.class, char[].class);
            } else {
                constructor =
                        String.class.getDeclaredConstructor(char[].class, boolean.class);
            }
            if (constructor != null) {
                constructor.setAccessible(true);
                return new FastStringCreator(constructor);
            }
        } catch (Throwable t) {
            Logger.
                    getLogger(UTFEncoderDecoder.class).
                    finest("No fast string creator seems to available, falling back to reflection", t);
        }
        return null;
    }

    private static class DefaultStringCreator implements UTFEncoderDecoder.StringCreator {

        @Override
        public String buildString(final char[] chars) {
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
        public String buildString(final char[] chars) {
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

        String buildString(final char[] chars);

    }

}
