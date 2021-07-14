/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains factory methods for creating AWS S3 sources.
 */
public final class S3Sources {

    private static final int LOCAL_PARALLELISM = 2;

    private S3Sources() {
    }

    /**
     * Convenience for {@link #s3(List, String, Charset, SupplierEx, BiFunctionEx)}.
     * Emits lines to downstream without any transformation and uses {@link
     * StandardCharsets#UTF_8}.
     */
    @Nonnull
    public static BatchSource<String> s3(
            @Nonnull List<String> bucketNames,
            @Nullable String prefix,
            @Nonnull SupplierEx<? extends S3Client> clientSupplier
    ) {
        return s3(bucketNames, prefix, UTF_8, clientSupplier, (name, line) -> line);
    }

    /**
     * Creates an AWS S3 {@link BatchSource} which lists all the objects in the
     * bucket-list using given {@code prefix}, reads them line by line,
     * transforms each line to the desired output object using given {@code
     * mapFn} and emits them to downstream.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * The default local parallelism for this processor is 2.
     * <p>
     * Here is an example which reads the objects from a single bucket with
     * applying the given prefix.
     *
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * BatchStage<String> srcStage = p.readFrom(S3Sources.s3(
     *      Arrays.asList("bucket1", "bucket2"),
     *      "prefix",
     *      StandardCharsets.UTF_8,
     *      () -> S3Client.create(),
     *      (filename, line) -> line
     * ));
     * }</pre>
     *
     * @param bucketNames    list of bucket-names
     * @param prefix         the prefix to filter the objects. Optional, passing
     *                       {@code null} will list all objects.
     * @param clientSupplier function which returns the s3 client to use
     *                       one client per processor instance is used
     * @param mapFn          the function which creates output object from each
     *                       line. Gets the object name and line as parameters
     * @param <T>            the type of the items the source emits
     */
    @Nonnull
    public static <T> BatchSource<T> s3(
            @Nonnull List<String> bucketNames,
            @Nullable String prefix,
            @Nonnull Charset charset,
            @Nonnull SupplierEx<? extends S3Client> clientSupplier,
            @Nonnull BiFunctionEx<String, String, ? extends T> mapFn
    ) {
        String charsetName = charset.name();

        FunctionEx<InputStream, Stream<String>> readFileFn = responseInputStream -> {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(responseInputStream, Charset.forName(charsetName)));

            return reader.lines();
        };

        return s3(bucketNames, prefix, clientSupplier, readFileFn, mapFn);
    }

    /**
     * Creates an AWS S3 {@link BatchSource} which lists all the objects in the
     * bucket-list using given {@code prefix}, reads them using provided {@code
     * readFileFn}, transforms each read item to the desired output object
     * using given {@code mapFn} and emits them to downstream.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * The default local parallelism for this processor is 2.
     * <p>
     * Here is an example which reads the objects from a single bucket with
     * applying the given prefix.
     *
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * BatchStage<String> srcStage = p.readFrom(S3Sources.s3(
     *      Arrays.asList("bucket1", "bucket2"),
     *      "prefix",
     *      () -> S3Client.create(),
     *      (inputStream) -> new LineIterator(new InputStreamReader(inputStream)),
     *      (filename, line) -> line
     * ));
     * }</pre>
     *
     * @param bucketNames    list of bucket-names
     * @param prefix         the prefix to filter the objects. Optional, passing
     *                       {@code null} will list all objects.
     * @param clientSupplier function which returns the s3 client to use
     *                       one client per processor instance is used
     * @param readFileFn     the function which creates iterator, which reads
     *                       the file in lazy way
     * @param mapFn          the function which creates output object from each
     *                       line. Gets the object name and line as parameters
     * @param <T>            the type of the items the source emits
     */
    @Nonnull
    public static <I, T> BatchSource<T> s3(
            @Nonnull List<String> bucketNames,
            @Nullable String prefix,
            @Nonnull SupplierEx<? extends S3Client> clientSupplier,
            @Nonnull FunctionEx<? super InputStream, ? extends Stream<I>> readFileFn,
            @Nonnull BiFunctionEx<String, ? super I, ? extends T> mapFn
    ) {
        TriFunction<? super InputStream, String, String, ? extends Stream<I>> adaptedFunction =
                (inputStream, key, bucketName) -> readFileFn.apply(inputStream);
        return SourceBuilder
                .batch("s3-source", context ->
                        new S3SourceContext<I, T>(bucketNames, prefix, context, clientSupplier, adaptedFunction,
                                mapFn))
                .<T>fillBufferFn(S3SourceContext::fillBuffer)
                .distributed(LOCAL_PARALLELISM)
                .destroyFn(S3SourceContext::close)
                .build();
    }

    /**
     * Creates an AWS S3 {@link BatchSource} which lists all the objects in the
     * bucket-list using given {@code prefix}, reads them using provided {@code
     * readFileFn}, transforms each read item to the desired output object
     * using given {@code mapFn} and emits them to downstream.
     * <p>
     * The source does not save any state to snapshot. If the job is restarted,
     * it will re-emit all entries.
     * <p>
     * The default local parallelism for this processor is 2.
     * <p>
     * Here is an example which reads the objects from a single bucket with
     * applying the given prefix.
     *
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * BatchStage<String> srcStage = p.readFrom(S3Sources.s3(
     *      Arrays.asList("bucket1", "bucket2"),
     *      "prefix",
     *      () -> S3Client.create(),
     *      (inputStream, key, bucketName) -> new LineIterator(new InputStreamReader(inputStream)),
     *      (filename, line) -> line
     * ));
     * }</pre>
     *
     * @param bucketNames    list of bucket-names
     * @param prefix         the prefix to filter the objects. Optional, passing
     *                       {@code null} will list all objects.
     * @param clientSupplier function which returns the s3 client to use
     *                       one client per processor instance is used
     * @param readFileFn     the function which creates iterator, which reads
     *                       the file in lazy way
     * @param mapFn          the function which creates output object from each
     *                       line. Gets the object name and line as parameters
     * @param <T>            the type of the items the source emits
     *
     * @since Jet 4.3
     */
    @Nonnull
    public static <I, T> BatchSource<T> s3(
            @Nonnull List<String> bucketNames,
            @Nullable String prefix,
            @Nonnull SupplierEx<? extends S3Client> clientSupplier,
            @Nonnull TriFunction<? super InputStream, String, String, ? extends Stream<I>> readFileFn,
            @Nonnull BiFunctionEx<String, ? super I, ? extends T> mapFn
    ) {
        return SourceBuilder
                .batch("s3Source", context ->
                        new S3SourceContext<I, T>(bucketNames, prefix, context, clientSupplier, readFileFn,
                                mapFn))
                .<T>fillBufferFn(S3SourceContext::fillBuffer)
                .distributed(LOCAL_PARALLELISM)
                .destroyFn(S3SourceContext::close)
                .build();
    }

    private static final class S3SourceContext<I, T> {

        private static final int BATCH_COUNT = 1024;

        private final String prefix;
        private final S3Client amazonS3;
        private final TriFunction<? super InputStream, String, String, ? extends Stream<I>> readFileFn;
        private final BiFunctionEx<String, ? super I, ? extends T> mapFn;
        private final int processorIndex;
        private final int totalParallelism;

        // (bucket, key)
        private Iterator<Entry<String, String>> objectIterator;
        private Traverser<I> itemTraverser;
        private String currentKey;

        private S3SourceContext(
                List<String> bucketNames,
                String prefix,
                Context context,
                SupplierEx<? extends S3Client> clientSupplier,
                TriFunction<? super InputStream, String, String, ? extends Stream<I>> readFileFn,
                BiFunctionEx<String, ? super I, ? extends T> mapFn
        ) {
            this.prefix = prefix;
            this.amazonS3 = clientSupplier.get();
            this.readFileFn = readFileFn;
            this.mapFn = mapFn;
            this.processorIndex = context.globalProcessorIndex();
            this.totalParallelism = context.totalParallelism();
            this.objectIterator = bucketNames
                    .stream()
                    .flatMap(bucket -> amazonS3.listObjectsV2Paginator(b ->
                            b.bucket(bucket).prefix(this.prefix)).contents().stream()
                                    .map(S3Object::key)
                                    .filter(this::belongsToThisProcessor)
                                    .map(key -> entry(bucket, key))
                    ).iterator();
        }

        private void fillBuffer(SourceBuffer<? super T> buffer) {
            if (itemTraverser != null) {
                addBatchToBuffer(buffer);
                return;
            }

            if (objectIterator.hasNext()) {
                Entry<String, String> entry = objectIterator.next();
                String bucketName = entry.getKey();
                String key = entry.getValue();
                GetObjectRequest getObjectRequest = GetObjectRequest
                        .builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();

                ResponseInputStream<GetObjectResponse> responseInputStream = amazonS3.getObject(getObjectRequest);
                currentKey = key;
                itemTraverser = traverseStream(readFileFn.apply(responseInputStream, key, bucketName));
                addBatchToBuffer(buffer);
            } else {
                // iterator is empty, we've exhausted all the objects
                buffer.close();
                objectIterator = null;
            }
        }

        private void addBatchToBuffer(SourceBuffer<? super T> buffer) {
            assert currentKey != null : "currentKey must not be null";
            for (int i = 0; i < BATCH_COUNT; i++) {
                I item = itemTraverser.next();
                if (item == null) {
                    itemTraverser = null;
                    currentKey = null;
                    return;
                }
                buffer.add(mapFn.apply(currentKey, item));
            }
        }

        private boolean belongsToThisProcessor(String key) {
            return Math.floorMod(key.hashCode(), totalParallelism) == processorIndex;
        }

        private void close() {
            amazonS3.close();
        }
    }
}
