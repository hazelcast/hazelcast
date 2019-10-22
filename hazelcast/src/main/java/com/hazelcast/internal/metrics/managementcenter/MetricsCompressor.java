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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MutableMetricDescriptor;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.MetricDescriptorImpl;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static java.lang.Math.multiplyExact;

/**
 * Helper class for compressing metrics into a {@code byte[]} blob to be
 * read by various consumers. The consumer itself is passed in as a
 * parameter.
 * <p>
 * The compressor uses dictionary based delta compression and deflates
 * the result {@code byte[]} by using {@link DeflaterOutputStream}. This
 * compressor doesn't use the textual representation of the {@link MetricDescriptor}
 * hence it is agnostic to the order of the tags in that representation.
 * <p>
 * Adding metrics by calling {@link #addLong(MetricDescriptor, long)} or
 * {@link #addDouble(MetricDescriptor, double)} builds a dictionary by
 * mapping all words found in the passed {@link MetricDescriptor}s to
 * {@code int}s and these {@code int}s will be written to the resulting
 * {@code byte[]} blob. Before these {@code int}s are written, the current
 * {@link MetricDescriptor} is compared to the previous one and only the
 * fields of the descriptor that are different from the previous will be
 * written to the metrics blob.
 * <p>
 * When the blob is retrieved from this compressor (when the metrics
 * collection cycle finishes) the dictionary is stored in the dictionary
 * blob. The compressor iterates over the words stored in the dictionary
 * in ascending order and writes them to the blob by skipping the first
 * N characters that are equal to the first N character of the previously
 * written word, hence using delta compression here too.
 * <p>
 * After both the metrics and the dictionary blob is constructed, they
 * are copied into a final blob in the following structure:
 *
 * +--------------------------+--------------------+
 * | Compressor version       |   2 bytes (short)  |
 * +--------------------------+--------------------+
 * | Size of dictionary blob  |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Dictionary blob          |   variable size    |
 * +--------------------------+--------------------+
 * | Metrics blob             |   variable size    |
 * +--------------------------+--------------------+
 */
public class MetricsCompressor {

    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:TrailingComment"})
    private static final int INITIAL_BUFFER_SIZE_METRICS = 2 << 11; // 2kB
    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:TrailingComment"})
    private static final int INITIAL_BUFFER_SIZE_DICTIONARY = 2 << 10; // 1kB
    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:TrailingComment"})
    private static final int INITIAL_BUFFER_SIZE_DESCRIPTION = 2 << 8; // 32B

    private static final int SIZE_FACTOR_NUMERATOR = 11;
    private static final int SIZE_FACTOR_DENOMINATOR = 10;

    private static final int MASK_PREFIX = 1;
    private static final int MASK_METRIC = 1 << 1;
    private static final int MASK_DISCRIMINATOR = 1 << 2;
    private static final int MASK_DISCRIMINATOR_VALUE = 1 << 3;
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final int MASK_UNIT = 1 << 4;
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final int MASK_TAG_COUNT = 1 << 5;

    private static final int BITS_IN_BYTE = 8;
    private static final int BYTE_MASK = 0xff;

    private static final short BINARY_FORMAT_VERSION = 1;
    private static final int NULL_DICTIONARY_ID = -1;
    private static final int NULL_UNIT = -1;
    private static final int SIZE_VERSION = 2;
    private static final int SIZE_DICTIONARY_BLOB = 4;

    private MetricsDictionary dictionary;

    // output streams for the blob containing the dictionary
    private DataOutputStream dictionaryDos;
    private MorePublicByteArrayOutputStream dictionaryBaos = new MorePublicByteArrayOutputStream(INITIAL_BUFFER_SIZE_DICTIONARY);

    // output streams for the blob containing the metrics
    private DataOutputStream metricDos;
    private MorePublicByteArrayOutputStream metricBaos = new MorePublicByteArrayOutputStream(INITIAL_BUFFER_SIZE_METRICS);

    // temporary buffer to avoid DeflaterOutputStream's extra byte[] allocations
    // when writing primitive fields
    private DataOutputStream tmpDos;
    private MorePublicByteArrayOutputStream tmpBaos = new MorePublicByteArrayOutputStream(INITIAL_BUFFER_SIZE_DESCRIPTION);

    private int count;
    private MetricDescriptorImpl lastDescriptor;

    public MetricsCompressor() {
        reset(INITIAL_BUFFER_SIZE_DICTIONARY, INITIAL_BUFFER_SIZE_METRICS);
        tmpDos = new DataOutputStream(tmpBaos);
    }

    public void addLong(MetricDescriptor descriptor, long value) {
        try {
            writeDescriptor(descriptor);
            tmpDos.writeByte(ValueType.LONG.ordinal());
            tmpDos.writeLong(value);
            metricDos.write(tmpBaos.internalBuffer(), 0, tmpBaos.size());
            tmpBaos.reset();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
    }

    public void addDouble(MetricDescriptor descriptor, double value) {
        try {
            writeDescriptor(descriptor);
            tmpDos.writeByte(ValueType.DOUBLE.ordinal());
            tmpDos.writeDouble(value);
            metricDos.write(tmpBaos.internalBuffer(), 0, tmpBaos.size());
            tmpBaos.reset();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void writeDescriptor(MetricDescriptor descriptor) throws IOException {
        int mask = calculateDescriptorMask(descriptor);
        tmpDos.writeShort(mask);

        if ((mask & MASK_PREFIX) == 0) {
            tmpDos.writeInt(getDictionaryId(descriptor.prefix()));
        }
        if ((mask & MASK_METRIC) == 0) {
            tmpDos.writeInt(getDictionaryId(descriptor.metric()));
        }
        if ((mask & MASK_DISCRIMINATOR) == 0) {
            tmpDos.writeInt(getDictionaryId(descriptor.discriminator()));
        }
        if ((mask & MASK_DISCRIMINATOR_VALUE) == 0) {
            tmpDos.writeInt(getDictionaryId(descriptor.discriminatorValue()));
        }
        if ((mask & MASK_UNIT) == 0) {
            ProbeUnit unit = descriptor.unit();
            if (unit != null) {
                tmpDos.writeByte(unit.ordinal());
            } else {
                tmpDos.writeByte(NULL_UNIT);
            }
        }
        if ((mask & MASK_TAG_COUNT) == 0) {
            tmpDos.writeByte(descriptor.tagCount());
        }
        // further compression is possible by writing only the different tags
        descriptor.readTags((BiConsumerEx<String, String>) (tag, tagValue) -> {
            tmpDos.writeInt(getDictionaryId(tag));
            tmpDos.writeInt(getDictionaryId(tagValue));
        });
        count++;
        lastDescriptor = copyDescriptor(descriptor, lastDescriptor);
    }

    private int calculateDescriptorMask(MetricDescriptor descriptor) {
        int mask = 0;
        if (lastDescriptor != null) {
            if (Objects.equals(descriptor.prefix(), lastDescriptor.prefix())) {
                mask |= MASK_PREFIX;
            }
            if (Objects.equals(descriptor.metric(), lastDescriptor.metric())) {
                mask |= MASK_METRIC;
            }
            if (Objects.equals(descriptor.discriminator(), lastDescriptor.discriminator())) {
                mask |= MASK_DISCRIMINATOR;
            }
            if (Objects.equals(descriptor.discriminatorValue(), lastDescriptor.discriminatorValue())) {
                mask |= MASK_DISCRIMINATOR_VALUE;
            }
            if (descriptor.unit() == lastDescriptor.unit()) {
                mask |= MASK_UNIT;
            }
            if (descriptor.tagCount() == lastDescriptor.tagCount()) {
                mask |= MASK_TAG_COUNT;
            }
        }
        return mask;
    }

    private static MetricDescriptorImpl copyDescriptor(MetricDescriptor from, MetricDescriptorImpl to) {
        final MetricDescriptorImpl target = to != null ? to : DEFAULT_DESCRIPTOR_SUPPLIER.get();

        target.reset();
        target.withPrefix(from.prefix())
              .withMetric(from.metric())
              .withDiscriminator(from.discriminator(), from.discriminatorValue())
              .withUnit(from.unit());

        from.readTags(target::withTag);
        return target;
    }

    private int getDictionaryId(String word) {
        if (word == null) {
            return NULL_DICTIONARY_ID;
        }
        return dictionary.getDictionaryId(word);
    }

    public byte[] getBlobAndReset() {
        byte[] blob = getRenderedBlob();

        int estimatedBytesDictionary = dictionaryBaos.size() * SIZE_FACTOR_NUMERATOR / SIZE_FACTOR_DENOMINATOR;
        int estimatedBytesMetrics = metricBaos.size() * SIZE_FACTOR_NUMERATOR / SIZE_FACTOR_DENOMINATOR;
        reset(estimatedBytesDictionary, estimatedBytesMetrics);
        return blob;
    }

    private void writeDictionary() throws IOException {
        Collection<MetricsDictionary.Word> words = dictionary.words();
        dictionaryDos.writeInt(words.size());
        String lastWord = "";
        for (MetricsDictionary.Word word : words) {

            String wordText = word.word();
            int maxCommonLen = Math.min(lastWord.length(), wordText.length());
            int commonLen = 0;
            boolean common = true;
            for (int i = 0; i < maxCommonLen && common; i++) {
                if (wordText.charAt(i) == lastWord.charAt(i)) {
                    commonLen++;
                } else {
                    common = false;
                }
            }

            int diffLen = wordText.length() - commonLen;
            tmpDos.writeInt(word.dictionaryId());
            tmpDos.writeByte(commonLen);
            tmpDos.writeByte(diffLen);
            for (int i = commonLen; i < wordText.length(); i++) {
                tmpDos.writeChar(wordText.charAt(i));
            }
            lastWord = wordText;
            dictionaryDos.write(tmpBaos.internalBuffer(), 0, tmpBaos.size());
            tmpBaos.reset();
        }
    }

    public int count() {
        return count;
    }

    private void reset(int estimatedBytesDictionary, int estimatedBytesMetrics) {
        Deflater dictionaryCompressor = new Deflater();
        dictionaryCompressor.setLevel(Deflater.BEST_SPEED);

        // shrink the `dictionaryBaos` if capacity is more than 50% larger than the estimated size
        if (dictionaryBaos.capacity() > multiplyExact(estimatedBytesDictionary, 3) / 2) {
            dictionaryBaos = new MorePublicByteArrayOutputStream(estimatedBytesDictionary);
        }
        dictionaryBaos.reset();
        dictionaryDos = new DataOutputStream(new DeflaterOutputStream(dictionaryBaos, dictionaryCompressor));

        Deflater metricsCompressor = new Deflater();
        metricsCompressor.setLevel(Deflater.BEST_SPEED);
        // shrink the `metricsBaos` if capacity is more than 50% larger than the estimated size
        if (metricBaos.capacity() > multiplyExact(estimatedBytesMetrics, 3) / 2) {
            metricBaos = new MorePublicByteArrayOutputStream(estimatedBytesMetrics);
        }
        metricBaos.reset();
        metricDos = new DataOutputStream(new DeflaterOutputStream(metricBaos, metricsCompressor));

        dictionary = new MetricsDictionary();
        count = 0;
        lastDescriptor = null;
    }

    private byte[] getRenderedBlob() {
        try {
            writeDictionary();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }

        try {
            dictionaryDos.close();
            metricDos.close();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
        byte[] dictionaryBytes = dictionaryBaos.toByteArray();
        byte[] metricsBytes = metricBaos.toByteArray();

        // version info + dictionary length + dictionary blob + metrics blob
        int completeSize = SIZE_VERSION + SIZE_DICTIONARY_BLOB + dictionaryBytes.length + metricsBytes.length;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(completeSize);
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.write((BINARY_FORMAT_VERSION >>> BITS_IN_BYTE) & BYTE_MASK);
            dos.write(BINARY_FORMAT_VERSION & BYTE_MASK);
            dos.writeInt(dictionaryBytes.length);
            dos.write(dictionaryBytes);
            dos.write(metricsBytes);
            dos.close();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }

        return baos.toByteArray();
    }

    public static Iterator<Metric> decompressingIterator(byte[] bytes) {
        return decompressingIterator(bytes, DEFAULT_DESCRIPTOR_SUPPLIER);
    }

    @SuppressFBWarnings("RR_NOT_CHECKED")
    public static Iterator<Metric> decompressingIterator(byte[] bytes, Supplier<? extends MutableMetricDescriptor> supplier) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        int version = (bais.read() << BITS_IN_BYTE) + bais.read();
        if (version != BINARY_FORMAT_VERSION) {
            throw new RuntimeException("Incorrect format, expected version " + BINARY_FORMAT_VERSION + ", got " + version);
        }

        DataInputStream dis = new DataInputStream(bais);

        try {
            int dictionaryBlobSize = dis.readInt();
            byte[] dictionaryBlob = new byte[dictionaryBlobSize];
            // blob size - version - dictionary size int - dictionary blob size
            int metricsBlobSize = bytes.length - SIZE_VERSION - SIZE_DICTIONARY_BLOB - dictionaryBlobSize;
            byte[] metricsBlob = new byte[metricsBlobSize];
            dis.read(dictionaryBlob);
            dis.read(metricsBlob);

            dis.close();

            String[] dictionary = readDictionary(dictionaryBlob);
            return getIterator(metricsBlob, dictionary, supplier);

        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }

    }

    @SuppressWarnings({"checkstyle:AnonInnerLength", "checkstyle:MethodLength"})
    private static Iterator<Metric> getIterator(byte[] metricsBlob, String[] dictionary,
                                                Supplier<? extends MutableMetricDescriptor> supplier) {
        DataInputStream dis = new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(metricsBlob)));

        return new Iterator<Metric>() {
            MutableMetricDescriptor lastDescriptor = supplier.get();
            Metric next;
            int lastTagCount;

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
                ProbeUnit[] units = ProbeUnit.values();
                MutableMetricDescriptor newDescriptor = supplier.get();
                try {
                    int mask = dis.readUnsignedShort();

                    // prefix
                    if ((mask & MASK_PREFIX) != 0) {
                        newDescriptor.withPrefix(lastDescriptor.prefix());
                    } else {
                        newDescriptor.withPrefix(readNextWord());
                    }

                    // metric
                    if ((mask & MASK_METRIC) != 0) {
                        newDescriptor.withMetric(lastDescriptor.metric());
                    } else {
                        newDescriptor.withMetric(readNextWord());
                    }

                    // discriminator
                    final String discriminator;
                    final String discriminatorValue;
                    if ((mask & MASK_DISCRIMINATOR) != 0) {
                        discriminator = lastDescriptor.discriminator();
                    } else {
                        discriminator = readNextWord();
                    }
                    if ((mask & MASK_DISCRIMINATOR_VALUE) != 0) {
                        discriminatorValue = lastDescriptor.discriminatorValue();
                    } else {
                        discriminatorValue = readNextWord();
                    }
                    newDescriptor.withDiscriminator(discriminator, discriminatorValue);

                    // unit
                    if ((mask & MASK_UNIT) != 0) {
                        newDescriptor.withUnit(lastDescriptor.unit());
                    } else {
                        int unitOrdinal = dis.readByte();
                        ProbeUnit unit = unitOrdinal != NULL_UNIT ? units[unitOrdinal] : null;
                        newDescriptor.withUnit(unit);
                    }

                    // tags
                    final int tagCount;
                    if ((mask & MASK_TAG_COUNT) != 0) {
                        tagCount = lastTagCount;
                    } else {
                        tagCount = dis.readUnsignedByte();
                    }
                    lastTagCount = tagCount;

                    for (int i = 0; i < tagCount; i++) {
                        int tagId = dis.readInt();
                        int tagValueId = dis.readInt();
                        String tag = dictionary[tagId];
                        String tagValue = dictionary[tagValueId];
                        newDescriptor.withTag(tag, tagValue);
                    }

                    lastDescriptor = newDescriptor;
                    next = readNextMetric();
                } catch (EOFException ignored) {
                    next = null;
                } catch (IOException e) {
                    // should never be thrown
                    throw new RuntimeException(e);
                }
            }

            private String readNextWord() throws IOException {
                int dictionaryId = dis.readInt();
                return dictionaryId != NULL_DICTIONARY_ID ? dictionary[dictionaryId] : null;
            }

            private Metric readNextMetric() throws IOException {
                ValueType type = ValueType.valueOf(dis.readByte());
                switch (type) {
                    case LONG:
                        return new Metric(lastDescriptor.copy(), type, dis.readLong());
                    case DOUBLE:
                        return new Metric(lastDescriptor.copy(), type, dis.readDouble());
                    default:
                        throw new IllegalStateException("Unexpected metric value type: " + type);
                }
            }
        };
    }

    private static String[] readDictionary(byte[] dictionaryBlob) throws IOException {
        DataInputStream dis = new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(dictionaryBlob)));
        int dictionarySize = dis.readInt();
        String[] dictionary = new String[dictionarySize];
        String lastWord = "";
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < dictionarySize; i++) {
            int dictionaryId = dis.readInt();
            byte commonLen = dis.readByte();
            byte diffLen = dis.readByte();

            for (int j = 0; j < commonLen; j++) {
                sb.append(lastWord.charAt(j));
            }

            for (int j = 0; j < diffLen; j++) {
                sb.append(dis.readChar());
            }
            String readWord = sb.toString();
            lastWord = readWord;
            dictionary[dictionaryId] = readWord;
            sb.delete(0, commonLen + diffLen);
        }

        return dictionary;
    }

    private static class MorePublicByteArrayOutputStream extends ByteArrayOutputStream {
        MorePublicByteArrayOutputStream(int size) {
            super(size);
        }

        int capacity() {
            return buf.length;
        }

        byte[] internalBuffer() {
            return buf;
        }
    }

    enum ValueType {

        // Note: Do not change the order, as .ordinal() values are used for serialization
        LONG, DOUBLE;

        private static final ValueType[] VALUE_TYPES;

        static {
            VALUE_TYPES = ValueType.values();
        }

        public static ValueType valueOf(int ordinal) {
            try {
                return VALUE_TYPES[ordinal];
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Unexpected ordinal value for ValueType: " + ordinal);
            }
        }

    }
}
