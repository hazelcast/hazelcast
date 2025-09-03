/*
 * Copyright 2025 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.s3;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.memory.MemoryUnit;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains factory methods for creating AWS S3 sinks.
 */
public final class S3Sinks {

    private S3Sinks() {
    }

    /**
     * Convenience for {@link #s3(String, String, Charset, SupplierEx, FunctionEx)}
     * Uses {@link Object#toString()} to convert the items to lines.
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            @Nonnull SupplierEx<? extends S3Client> clientSupplier
    ) {
        return s3(bucketName, null, StandardCharsets.UTF_8, clientSupplier, Object::toString);
    }

    /**
     * Creates an AWS S3 {@link Sink} which writes items to files into the
     * given bucket. Sink converts each item to string using given {@code
     * toStringFn} and writes it as a line. The sink creates a file
     * in the bucket for each processor instance. Name of the file will include
     * an user provided prefix (if defined) and processor's global index,
     * for example the processor having the
     * index 2 with prefix {@code my-object-} will create the object
     * {@code my-object-2}.
     * <p>
     * No state is saved to snapshot for this sink. If the job is restarted
     * previously written files will be overwritten.
     * <p>
     * The default local parallelism for this sink is 1.
     * <p>
     * Here is an example which reads from a map and writes the entries
     * to given bucket using {@link Object#toString()} to convert the
     * values to a line.
     *
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * p.readFrom(Sources.map("map"))
     *  .writeTo(S3Sinks.s3("bucket", "my-map-", StandardCharsets.UTF_8,
     *      () -> S3Client.create(),
     *      Object::toString
     * ));
     * }</pre>
     *
     * @param <T>            type of the items the sink accepts
     * @param bucketName     the name of the bucket
     * @param prefix         the prefix to be included in the file name
     * @param charset        the charset to be used when encoding the strings
     * @param clientSupplier S3 client supplier
     * @param toStringFn     the function which converts each item to its
     *                       string representation
     */
    @Nonnull
    public static <T> Sink<? super T> s3(
            @Nonnull String bucketName,
            @Nullable String prefix,
            @Nonnull Charset charset,
            @Nonnull SupplierEx<? extends S3Client> clientSupplier,
            @Nonnull FunctionEx<? super T, String> toStringFn

    ) {
        String charsetName = charset.name();
        return SinkBuilder
                .sinkBuilder("s3Sink", context ->
                        new S3SinkContext<>(bucketName, prefix, charsetName, context.globalProcessorIndex(),
                                toStringFn, clientSupplier))
                .<T>receiveFn(S3SinkContext::receive)
                .flushFn(S3SinkContext::flush)
                .destroyFn(S3SinkContext::close)
                .build();
    }

    static final class S3SinkContext<T> {

        static final int DEFAULT_MAXIMUM_PART_NUMBER = 10000;
        static final int MINIMUM_PART_NUMBER = 1;
        // visible for testing
        static int maximumPartNumber = DEFAULT_MAXIMUM_PART_NUMBER;

        // the minimum size required for each part in AWS multipart
        static final int DEFAULT_MINIMUM_UPLOAD_PART_SIZE = (int) MemoryUnit.MEGABYTES.toBytes(5);
        static final double BUFFER_SCALE = 1.2d;

        private final String bucketName;
        private final String prefix;
        private final int processorIndex;
        private final S3Client s3Client;
        private final FunctionEx<? super T, String> toStringFn;
        private final Charset charset;
        private final byte[] lineSeparatorBytes;
        private final List<CompletedPart> completedParts = new ArrayList<>();

        private ByteBuffer buffer;
        private int partNumber = MINIMUM_PART_NUMBER; // must be between 1 and maximumPartNumber
        private int fileNumber;
        private String uploadId;

        private S3SinkContext(
                String bucketName,
                @Nullable String prefix,
                String charsetName, int processorIndex,
                FunctionEx<? super T, String> toStringFn,
                SupplierEx<? extends S3Client> clientSupplier) {
            this.bucketName = bucketName;
            String trimmedPrefix = StringUtil.strip(prefix);
            this.prefix = StringUtil.isNullOrEmpty(trimmedPrefix) ? "" : trimmedPrefix;
            this.processorIndex = processorIndex;
            this.s3Client = clientSupplier.get();
            this.toStringFn = toStringFn;
            this.charset = Charset.forName(charsetName);
            this.lineSeparatorBytes = System.lineSeparator().getBytes(charset);
            checkIfBucketExists();
            resizeBuffer(DEFAULT_MINIMUM_UPLOAD_PART_SIZE);
        }

        private void initiateUpload() {
            CreateMultipartUploadRequest req = CreateMultipartUploadRequest
                    .builder()
                    .bucket(bucketName)
                    .key(key())
                    .build();

            uploadId = s3Client.createMultipartUpload(req).uploadId();
        }

        private void checkIfBucketExists() {
            s3Client.getBucketLocation(b -> b.bucket(bucketName));
        }

        private void receive(T item) {
            byte[] bytes = toStringFn.apply(item).getBytes(charset);
            int length = bytes.length + lineSeparatorBytes.length;

            // not enough space in buffer to write
            if (buffer.remaining() < length) {
                // we try to flush the current buffer first
                flush();
                // this might not be enough - either item is bigger than current
                // buffer size or there was not enough data in the buffer to upload
                // in this case we have to resize the buffer to hold more data
                if (buffer.remaining() < length) {
                    resizeBuffer(length + buffer.position());
                }
            }

            buffer.put(bytes);
            buffer.put(lineSeparatorBytes);
        }

        private void resizeBuffer(int minimumLength) {
            assert buffer == null || buffer.position() < minimumLength;

            int newCapacity = (int) (minimumLength * BUFFER_SCALE);
            ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCapacity);
            if (buffer != null) {
                buffer.flip();
                newBuffer.put(buffer);
            }
            buffer = newBuffer;
        }

        private void flush() {
            if (uploadId == null) {
                initiateUpload();
            }
            if (buffer.position() > DEFAULT_MINIMUM_UPLOAD_PART_SIZE) {
                boolean isLastPart = partNumber == maximumPartNumber;
                flushBuffer(isLastPart);
            }
        }

        private void close() {
            try {
                flushBuffer(true);
            } finally {
                s3Client.close();
            }
        }

        private void flushBuffer(boolean isLastPart) {
            if (buffer.position() > 0) {
                buffer.flip();
                UploadPartRequest req = UploadPartRequest
                        .builder()
                        .bucket(bucketName)
                        .key(key())
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .build();

                String eTag = s3Client.uploadPart(req, RequestBody.fromByteBuffer(buffer)).eTag();
                completedParts.add(CompletedPart.builder().partNumber(partNumber).eTag(eTag).build());
                partNumber++;
                buffer.clear();
            }

            if (isLastPart) {
                completeUpload();
            }
        }

        private void completeUpload() {
            try {
                if (completedParts.isEmpty()) {
                    abortUpload();
                } else {
                    CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest
                            .builder()
                            .bucket(bucketName)
                            .key(key())
                            .uploadId(uploadId)
                            .multipartUpload(b -> b.parts(completedParts))
                            .build();

                    s3Client.completeMultipartUpload(req);
                    completedParts.clear();
                    partNumber = MINIMUM_PART_NUMBER;
                    uploadId = null;
                    fileNumber++;
                }
            } catch (Exception e) {
                abortUpload();
                ExceptionUtil.rethrow(e);
            }
        }

        private void abortUpload() {
            s3Client.abortMultipartUpload(b -> b.uploadId(uploadId).bucket(bucketName).key(key()));
        }

        private String key() {
            return prefix + processorIndex + (fileNumber == 0 ? "" : "." + fileNumber);
        }
    }
}
