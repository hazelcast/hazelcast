/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.managementcenter;

import com.hazelcast.internal.nio.Bits;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.internal.metrics.MetricsUtil.escapeMetricNamePart;
import static java.lang.Math.multiplyExact;

/**
 * Helper class for compressing metrics into a byte[] blob to be read by
 * various consumers. The consumer itself is passed in as a parameter.
 * <p>
 * Most efficient when the metrics to be compressed are fed in sorted by
 * name. It will work even if this condition is violated, but it won't
 * nearly be as efficient.
 * <p>
 * Before compressing it also converts legacy metric names to
 * {@code [metric=<oldName>]}.
 * <p>
 * The utility is optimized to repeatedly compress similar set of metrics: it
 * reuses the buffer and keeps it about 10% bigger than what was needed the
 * last time.
 */
public class MetricsCompressor {

    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:TrailingComment"})
    private static final int INITIAL_BUFFER_SIZE = 2 << 11; // 2kB
    private static final int SIZE_FACTOR_NUMERATOR = 11;
    private static final int SIZE_FACTOR_DENOMINATOR = 10;

    private static final int BITS_IN_BYTE = 8;
    private static final int BYTE_MASK = 0xff;

    private static final short SHORT_BITS = BITS_IN_BYTE * Bits.SHORT_SIZE_IN_BYTES;
    // required precision after the decimal point for doubles
    private static final int CONVERSION_PRECISION = 4;
    // coefficient for converting doubles to long
    private static final double DOUBLE_TO_LONG = Math.pow(10, CONVERSION_PRECISION);

    private static final short BINARY_FORMAT_VERSION = 1;

    private DataOutputStream dos;
    private MorePublicByteArrayOutputStream baos = new MorePublicByteArrayOutputStream(INITIAL_BUFFER_SIZE);
    private String lastName;
    private int count;

    public MetricsCompressor() {
        reset(INITIAL_BUFFER_SIZE);
    }

    public void addLong(String name, long value) {
        try {
            writeName(name);
            dos.writeLong(value);
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
    }

    public void addDouble(String name, double value) {
        try {
            writeName(name);
            // convert to long with specified precision
            dos.writeLong(Math.round(value * DOUBLE_TO_LONG));
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
    }

    public byte[] getBlobAndReset() {
        byte[] blob = getRenderedBlob();
        reset(blob.length * SIZE_FACTOR_NUMERATOR / SIZE_FACTOR_DENOMINATOR);
        return blob;
    }

    public int count() {
        return count;
    }

    private void writeName(String name) throws IOException {
        // Metric name should have the form "[tag1=value1,tag2=value2,...]". If it is not
        // enclosed in "[]", we convert it to "[metric=originalName]".
        if (!name.startsWith("[") || !name.endsWith("]")) {
            name = "[metric=" + escapeMetricNamePart(name) + ']';
        }

        if (name.length() >= 1 << SHORT_BITS) {
            throw new RuntimeException("metric name too long: " + name);
        }
        // count number of shared characters
        int equalPrefixLength = 0;
        int shorterLen = Math.min(name.length(), lastName.length());
        while (equalPrefixLength < shorterLen
                && name.charAt(equalPrefixLength) == lastName.charAt(equalPrefixLength)) {
            equalPrefixLength++;
        }
        dos.writeShort(equalPrefixLength);
        dos.writeUTF(name.substring(equalPrefixLength));
        lastName = name;
        count++;
    }

    private void reset(int estimatedBytes) {
        Deflater compressor = new Deflater();
        compressor.setLevel(Deflater.BEST_SPEED);
        // shrink the `baos` if capacity is more than 50% larger than the estimated size
        if (baos.capacity() > multiplyExact(estimatedBytes, 3) / 2) {
            baos = new MorePublicByteArrayOutputStream(estimatedBytes);
        }
        baos.reset();
        baos.write((BINARY_FORMAT_VERSION >>> BITS_IN_BYTE) & BYTE_MASK);
        baos.write(BINARY_FORMAT_VERSION & BYTE_MASK);
        dos = new DataOutputStream(new DeflaterOutputStream(baos, compressor));
        count = 0;
        lastName = "";
    }

    private byte[] getRenderedBlob() {
        try {
            dos.close();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    @SuppressWarnings("checkstyle:AnonInnerLength")
    public static Iterator<Metric> decompressingIterator(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        int version = (bais.read() << BITS_IN_BYTE) + bais.read();
        if (version != BINARY_FORMAT_VERSION) {
            throw new RuntimeException("Incorrect format, expected version " + BINARY_FORMAT_VERSION
                    + ", got " + version);
        }
        DataInputStream dis = new DataInputStream(new InflaterInputStream(bais));

        return new Iterator<Metric>() {
            String lastName = "";
            Metric next;

            {
                moveNext();
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Metric next() {
                try {
                    return next;
                } finally {
                    moveNext();
                }
            }

            private void moveNext() {
                try {
                    int equalPrefixLen;
                    try {
                        equalPrefixLen = dis.readUnsignedShort();
                    } catch (EOFException ignored) {
                        next = null;
                        dis.close();
                        return;
                    }
                    lastName = lastName.substring(0, equalPrefixLen) + dis.readUTF();
                    next = new Metric(lastName, dis.readLong());
                } catch (IOException e) {
                    // unexpected EOFException can occur here
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private static class MorePublicByteArrayOutputStream extends ByteArrayOutputStream {
        MorePublicByteArrayOutputStream(int size) {
            super(size);
        }

        int capacity() {
            return buf.length;
        }
    }

}
