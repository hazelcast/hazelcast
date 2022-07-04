/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.ProbeUnit;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
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
 * +--------------------------------+--------------------+
 * | Compressor version             |   2 bytes (short)  |
 * +--------------------------------+--------------------+
 * | Size of dictionary blob        |   4 bytes (int)    |
 * +--------------------------------+--------------------+
 * | Dictionary blob                |   variable size    |
 * +--------------------------------+--------------------+
 * | Number of metrics in the blob  |   4 bytes (int)    |
 * +--------------------------------+--------------------+
 * | Metrics blob                   |   variable size    |
 * +--------------------------------+--------------------+
 */
public class MetricsCompressor {

    static final int UNSIGNED_BYTE_MAX_VALUE = 255;

    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:TrailingComment"})
    private static final int INITIAL_BUFFER_SIZE_METRICS = 2 << 11; // 4kB
    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:TrailingComment"})
    private static final int INITIAL_BUFFER_SIZE_DICTIONARY = 2 << 10; // 2kB
    @SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:TrailingComment"})
    private static final int INITIAL_BUFFER_SIZE_DESCRIPTION = 2 << 8; // 512B

    private static final int SIZE_FACTOR_NUMERATOR = 11;
    private static final int SIZE_FACTOR_DENOMINATOR = 10;

    private static final int MASK_PREFIX = 1;
    private static final int MASK_METRIC = 1 << 1;
    private static final int MASK_DISCRIMINATOR = 1 << 2;
    private static final int MASK_DISCRIMINATOR_VALUE = 1 << 3;
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final int MASK_UNIT = 1 << 4;
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final int MASK_EXCLUDED_TARGETS = 1 << 5;
    @SuppressWarnings("checkstyle:MagicNumber")
    private static final int MASK_TAG_COUNT = 1 << 6;

    private static final int BITS_IN_BYTE = 8;
    private static final int BYTE_MASK = 0xff;

    private static final short BINARY_FORMAT_VERSION = 1;
    private static final int NULL_DICTIONARY_ID = -1;
    private static final int NULL_UNIT = -1;
    private static final int SIZE_VERSION = 2;
    private static final int SIZE_DICTIONARY_BLOB = 4;
    private static final int SIZE_COUNT_METRICS = 4;

    private MetricsDictionary dictionary;

    // output streams for the blob containing the dictionary
    private DataOutputStream dictionaryDos;
    private Deflater dictionaryCompressor;
    private MorePublicByteArrayOutputStream dictionaryBaos = new MorePublicByteArrayOutputStream(INITIAL_BUFFER_SIZE_DICTIONARY);

    // output streams for the blob containing the metrics
    private DataOutputStream metricDos;
    private Deflater metricsCompressor;
    private MorePublicByteArrayOutputStream metricBaos = new MorePublicByteArrayOutputStream(INITIAL_BUFFER_SIZE_METRICS);

    // temporary buffer to avoid DeflaterOutputStream's extra byte[] allocations
    // when writing primitive fields
    private final DataOutputStream tmpDos;
    private final MorePublicByteArrayOutputStream tmpBaos = new MorePublicByteArrayOutputStream(INITIAL_BUFFER_SIZE_DESCRIPTION);

    private int count;
    private MetricDescriptor lastDescriptor;

    public MetricsCompressor() {
        reset(INITIAL_BUFFER_SIZE_DICTIONARY, INITIAL_BUFFER_SIZE_METRICS);
        tmpDos = new DataOutputStream(tmpBaos);
    }

    public void addLong(MetricDescriptor descriptor, long value) throws LongWordException {
        try {
            tmpBaos.reset();
            writeDescriptor(descriptor);
            tmpDos.writeByte(ValueType.LONG.ordinal());
            tmpDos.writeLong(value);
            metricDos.write(tmpBaos.internalBuffer(), 0, tmpBaos.size());
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
    }

    public void addDouble(MetricDescriptor descriptor, double value) throws LongWordException {
        try {
            tmpBaos.reset();
            writeDescriptor(descriptor);
            tmpDos.writeByte(ValueType.DOUBLE.ordinal());
            tmpDos.writeDouble(value);
            metricDos.write(tmpBaos.internalBuffer(), 0, tmpBaos.size());
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void writeDescriptor(MetricDescriptor originalDescriptor) throws IOException, LongWordException {
        MetricDescriptor descriptor = prepareDescriptor(originalDescriptor);
        int mask = calculateDescriptorMask(descriptor);
        tmpDos.writeByte(mask);

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
        if ((mask & MASK_EXCLUDED_TARGETS) == 0) {
            tmpDos.writeByte(MetricTarget.bitset(descriptor.excludedTargets()));
        }
        if ((mask & MASK_TAG_COUNT) == 0) {
            tmpDos.writeByte(descriptor.tagCount());
        }
        // further compression is possible by writing only the different tags
        for (int i = 0; i < descriptor.tagCount(); i++) {
            String tag = descriptor.tag(i);
            String tagValue = descriptor.tagValue(i);
            tmpDos.writeInt(getDictionaryId(tag));
            tmpDos.writeInt(getDictionaryId(tagValue));
        }
        count++;
        lastDescriptor = copyDescriptor(descriptor, lastDescriptor);
    }

    private MetricDescriptor prepareDescriptor(MetricDescriptor descriptor) {
        final ProbeUnit unit = descriptor.unit();
        if (unit == null || !unit.isNewUnit()) {
            return descriptor;
        } else {
            return descriptor
                    .copy()
                    .withTag("metric-unit", unit.name())
                    .withUnit(null);
        }
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
            if (Objects.equals(descriptor.excludedTargets(), lastDescriptor.excludedTargets())) {
                mask |= MASK_EXCLUDED_TARGETS;
            }
            if (descriptor.tagCount() == lastDescriptor.tagCount()) {
                mask |= MASK_TAG_COUNT;
            }
        }
        return mask;
    }

    private static MetricDescriptor copyDescriptor(MetricDescriptor from, MetricDescriptor to) {
        final MetricDescriptor target = to != null ? to : DEFAULT_DESCRIPTOR_SUPPLIER.get();

        return target.copy(from);
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
            tmpBaos.reset();
            String wordText = word.word();
            if (wordText.length() > UNSIGNED_BYTE_MAX_VALUE) {
                // this should have been checked earlier, this is a safety check
                throw new RuntimeException("Dictionary element too long: " + wordText);
            }
            int maxCommonLen = Math.min(lastWord.length(), wordText.length());
            int commonLen = 0;
            while (commonLen < maxCommonLen
                    && wordText.charAt(commonLen) == lastWord.charAt(commonLen)) {
                commonLen++;
            }

            int diffLen = wordText.length() - commonLen;
            // we write to tmpDos to avoid the allocation of byte[1] in DeflaterOutputStream.write(int)
            tmpDos.writeInt(word.dictionaryId());
            tmpDos.writeByte(commonLen);
            tmpDos.writeByte(diffLen);
            for (int i = commonLen; i < wordText.length(); i++) {
                tmpDos.writeChar(wordText.charAt(i));
            }
            lastWord = wordText;
            dictionaryDos.write(tmpBaos.internalBuffer(), 0, tmpBaos.size());
        }
    }

    public int count() {
        return count;
    }

    private void reset(int estimatedBytesDictionary, int estimatedBytesMetrics) {
        dictionaryCompressor = new Deflater();
        dictionaryCompressor.setLevel(Deflater.BEST_SPEED);

        // shrink the `dictionaryBaos` if capacity is more than 50% larger than the estimated size
        if (dictionaryBaos.capacity() > multiplyExact(estimatedBytesDictionary, 3) / 2) {
            dictionaryBaos = new MorePublicByteArrayOutputStream(estimatedBytesDictionary);
        }
        dictionaryBaos.reset();
        dictionaryDos = new DataOutputStream(new DeflaterOutputStream(dictionaryBaos, dictionaryCompressor));

        metricsCompressor = new Deflater();
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
            dictionaryDos.close();
            dictionaryCompressor.end();
            metricDos.close();
            metricsCompressor.end();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
        byte[] dictionaryBytes = dictionaryBaos.toByteArray();
        byte[] metricsBytes = metricBaos.toByteArray();

        // version info + dictionary length + dictionary blob + number of metrics + metrics blob
        int completeSize =
                SIZE_VERSION + SIZE_DICTIONARY_BLOB + dictionaryBytes.length + SIZE_COUNT_METRICS + metricsBytes.length;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(completeSize);
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.write((BINARY_FORMAT_VERSION >>> BITS_IN_BYTE) & BYTE_MASK);
            dos.write(BINARY_FORMAT_VERSION & BYTE_MASK);
            dos.writeInt(dictionaryBytes.length);
            dos.write(dictionaryBytes);
            dos.writeInt(count);
            dos.write(metricsBytes);
            dos.close();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }

        return baos.toByteArray();
    }

    public static void extractMetrics(byte[] blob, MetricConsumer consumer) {
        extractMetrics(blob, consumer, DEFAULT_DESCRIPTOR_SUPPLIER);
    }

    @SuppressFBWarnings("RR_NOT_CHECKED")
    public static void extractMetrics(byte[] blob, MetricConsumer consumer, Supplier<? extends MetricDescriptor> supplier) {
        ByteArrayInputStream bais = new ByteArrayInputStream(blob);
        int version = (bais.read() << BITS_IN_BYTE) + bais.read();
        if (version != BINARY_FORMAT_VERSION) {
            throw new RuntimeException("Incorrect format, expected version " + BINARY_FORMAT_VERSION + ", got " + version);
        }

        DataInputStream dis = new DataInputStream(bais);

        try {
            int dictionaryBlobSize = dis.readInt();
            byte[] dictionaryBlob = new byte[dictionaryBlobSize];
            dis.read(dictionaryBlob);

            int countMetrics = dis.readInt();
            // blob size - version - dictionary size int - dictionary blob size - count of metrics size
            int metricsBlobSize = blob.length - SIZE_VERSION - SIZE_DICTIONARY_BLOB - dictionaryBlobSize - SIZE_COUNT_METRICS;
            byte[] metricsBlob = new byte[metricsBlobSize];
            dis.read(metricsBlob);

            dis.close();

            String[] dictionary = readDictionary(dictionaryBlob);
            new MetricsDecompressor(metricsBlob, countMetrics, dictionary, supplier, consumer).extractMetrics();
        } catch (IOException e) {
            // should never be thrown
            throw new RuntimeException(e);
        }
    }

    private static String[] readDictionary(byte[] dictionaryBlob) throws IOException {
        try (DataInputStream dis = new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(dictionaryBlob)))) {
            int dictionarySize = dis.readInt();
            String[] dictionary = new String[dictionarySize];
            String lastWord = "";
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < dictionarySize; i++) {
                int dictionaryId = dis.readInt();
                int commonLen = dis.readUnsignedByte();
                int diffLen = dis.readUnsignedByte();
                sb.append(lastWord, 0, commonLen);
                for (int j = 0; j < diffLen; j++) {
                    sb.append(dis.readChar());
                }
                String readWord = sb.toString();
                lastWord = readWord;
                dictionary[dictionaryId] = readWord;
                sb.setLength(0);
            }

            return dictionary;
        }
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

    private static final class MetricsDecompressor {
        private final int countMetrics;
        private final MetricConsumer consumer;
        private final String[] dictionary;
        private final DataInputStream dis;
        private final Supplier<? extends MetricDescriptor> supplier;
        private final MetricDescriptor lastDescriptor;
        private final ProbeUnit[] units = ProbeUnit.values();

        private MetricsDecompressor(byte[] metricsBlob, int countMetrics, String[] dictionary,
                                    Supplier<? extends MetricDescriptor> supplier,
                                    MetricConsumer consumer) {
            this.countMetrics = countMetrics;
            this.consumer = consumer;
            this.dictionary = dictionary;
            this.dis = new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(metricsBlob)));
            this.supplier = supplier;
            this.lastDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER.get();
        }

        private void extractMetrics() throws IOException {
            try {
                for (int i = 0; i < countMetrics; i++) {
                    MetricDescriptor descriptor = readMetricDescriptor();
                    lastDescriptor.copy(descriptor);

                    int typeOrdinal = dis.readUnsignedByte();
                    ValueType type = ValueType.valueOf(typeOrdinal);

                    switch (type) {
                        case LONG:
                            consumer.consumeLong(descriptor, dis.readLong());
                            break;
                        case DOUBLE:
                            consumer.consumeDouble(descriptor, dis.readDouble());
                            break;
                        default:
                            throw new IllegalStateException("Unexpected metric value type: "
                                    + type + " with ordinal " + typeOrdinal);
                    }
                }
            } finally {
                dis.close();
            }
        }

        private MetricDescriptor readMetricDescriptor() throws IOException {
            int mask = dis.readUnsignedByte();
            MetricDescriptor descriptor = supplier.get();
            fillPrefix(descriptor, mask);
            fillMetric(descriptor, mask);
            fillDiscriminator(descriptor, mask);
            fillUnit(descriptor, mask);
            fillExcludedTargets(descriptor, mask);
            fillTags(descriptor, mask);

            return descriptor;
        }

        private void fillPrefix(MetricDescriptor descriptor, int mask) throws IOException {
            if ((mask & MASK_PREFIX) != 0) {
                descriptor.withPrefix(lastDescriptor.prefix());
            } else {
                descriptor.withPrefix(readNextWord());
            }
        }

        private void fillMetric(MetricDescriptor descriptor, int mask) throws IOException {
            if ((mask & MASK_METRIC) != 0) {
                descriptor.withMetric(lastDescriptor.metric());
            } else {
                descriptor.withMetric(readNextWord());
            }
        }

        private void fillDiscriminator(MetricDescriptor descriptor, int mask) throws IOException {
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
            descriptor.withDiscriminator(discriminator, discriminatorValue);
        }

        private void fillUnit(MetricDescriptor descriptor, int mask) throws IOException {
            if ((mask & MASK_UNIT) != 0) {
                descriptor.withUnit(lastDescriptor.unit());
            } else {
                int unitOrdinal = dis.readByte();
                // we protect against using a unit that is introduced on the producer, meaning not in
                // the array of the known units
                // if the consumer doesn't know it attempting to use the unit would result
                // in IndexOutOfBoundsException
                ProbeUnit unit = unitOrdinal != NULL_UNIT && unitOrdinal < units.length ? units[unitOrdinal] : null;
                descriptor.withUnit(unit);
            }
        }

        private void fillExcludedTargets(MetricDescriptor descriptor, int mask) throws IOException {
            if ((mask & MASK_EXCLUDED_TARGETS) != 0) {
                descriptor.withExcludedTargets(lastDescriptor.excludedTargets());
            } else {
                int excludedMetricsBitset = dis.readByte();
                descriptor.withExcludedTargets(MetricTarget.asSet(excludedMetricsBitset));
            }
        }

        private void fillTags(MetricDescriptor descriptor, int mask) throws IOException {
            final int tagCount;
            if ((mask & MASK_TAG_COUNT) != 0) {
                tagCount = lastDescriptor.tagCount();
            } else {
                tagCount = dis.readUnsignedByte();
            }

            for (int i = 0; i < tagCount; i++) {
                int tagId = dis.readInt();
                int tagValueId = dis.readInt();
                String tag = dictionary[tagId];
                String tagValue = dictionary[tagValueId];
                descriptor.withTag(tag, tagValue);
            }
        }

        private String readNextWord() throws IOException {
            int dictionaryId = dis.readInt();
            return dictionaryId != NULL_DICTIONARY_ID ? dictionary[dictionaryId] : null;
        }
    }
}
