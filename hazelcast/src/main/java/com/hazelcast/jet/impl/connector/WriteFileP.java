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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.processor.UnboundedTransactionsProcessorUtility;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.security.permission.ConnectorPermission;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.FileSinkBuilder.DISABLE_ROLLING;
import static com.hazelcast.jet.pipeline.FileSinkBuilder.TEMP_FILE_SUFFIX;
import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;

/**
 * A file-writing sink supporting rolling files. Rolling can occur:
 * <ul>
 *      <li>at a snapshot boundary
 *      <li>when a file size is reached
 *      <li>when a date changes
 * </ul>
 */
public final class WriteFileP<T> implements Processor {

    private static final LongSupplier SYSTEM_CLOCK = (LongSupplier & Serializable) System::currentTimeMillis;
    /**
     * A pattern to parse processor index from a file name. File name has the form:
     * [<date>-]<global processor index>[-<sequence>][".tmp"]
     * This regexp assumes the sequence is present and searches from the right
     * because the structure of the date is user-supplied and can by anything.
     */
    private static final Pattern FILE_INDEX_WITH_SEQ = Pattern.compile("(\\d+)-\\d+(\\.tmp)?$");

    private final Path directory;
    private final FunctionEx<? super T, ? extends String> toStringFn;
    private final Charset charset;
    private final DateTimeFormatter dateFormatter;
    private final long maxFileSize;
    private final boolean exactlyOnce;
    private final LongSupplier clock;

    private UnboundedTransactionsProcessorUtility<FileId, FileResource> utility;
    private Context context;
    private int fileSequence;
    private SizeTrackingStream sizeTrackingStream;
    private String lastFileDate;

    /**
     * Rolling by date is based on system clock, not on event time.
     */
    public WriteFileP(
            @Nonnull String directoryName,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull String charset,
            @Nullable String dateFormatter,
            long maxFileSize,
            boolean exactlyOnce,
            @Nonnull LongSupplier clock
    ) {
        this.directory = Paths.get(directoryName);
        this.toStringFn = toStringFn;
        this.charset = Charset.forName(charset);
        this.dateFormatter = dateFormatter != null
                ? DateTimeFormatter.ofPattern(dateFormatter).withZone(ZoneId.from(ZoneOffset.UTC))
                : null;
        this.maxFileSize = maxFileSize;
        this.exactlyOnce = exactlyOnce;
        this.clock = clock;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws IOException {
        this.context = context;
        Files.createDirectories(directory);

        ProcessingGuarantee guarantee = context.processingGuarantee() == EXACTLY_ONCE && !exactlyOnce
                ? AT_LEAST_ONCE
                : context.processingGuarantee();
        utility = new UnboundedTransactionsProcessorUtility<>(
                outbox,
                context,
                guarantee,
                this::newFileName,
                this::newFileResource,
                this::recoverAndCommit,
                this::abortUnfinishedTransactions
        );
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean tryProcess() {
        return utility.tryProcess();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        // roll file on date change
        if (dateFormatter != null && !currentTimeFormatted().equals(lastFileDate)) {
            fileSequence = 0;
            utility.finishActiveTransaction();
        }
        FileResource transaction = utility.activeTransaction();
        Writer writer = transaction.writer();
        try {
            for (Object item; (item = inbox.poll()) != null; ) {
                @SuppressWarnings("unchecked")
                T castedItem = (T) item;
                writer.write(toStringFn.apply(castedItem));
                writer.write(System.lineSeparator());
                if (maxFileSize != DISABLE_ROLLING && sizeTrackingStream.size >= maxFileSize) {
                    utility.finishActiveTransaction();
                    writer = utility.activeTransaction().writer();
                }
            }
            writer.flush();
            if (maxFileSize != DISABLE_ROLLING && sizeTrackingStream.size >= maxFileSize) {
                utility.finishActiveTransaction();
            }
        } catch (IOException e) {
            throw new RestartableException(e);
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean complete() {
        utility.afterCompleted();
        return true;
    }

    @Override
    public void close() {
        if (utility != null) {
            utility.close();
        }
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return utility.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        return utility.snapshotCommitFinish(success);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        utility.restoreFromSnapshot(inbox);
    }

    private FileId newFileName() {
        StringBuilder sb = new StringBuilder();
        if (dateFormatter != null) {
            lastFileDate = currentTimeFormatted();
            sb.append(lastFileDate);
            sb.append('-');
        }
        sb.append(context.globalProcessorIndex());
        String file = sb.toString();
        boolean usesSequence = utility.externalGuarantee() == EXACTLY_ONCE || maxFileSize != DISABLE_ROLLING;
        if (usesSequence) {
            // check existing files if the sequence number is used
            int prefixLength = sb.length();
            do {
                sb.append('-').append(fileSequence++);
                file = sb.toString();
                sb.setLength(prefixLength);
            } while (Files.exists(directory.resolve(file))
                    || Files.exists(directory.resolve(file + TEMP_FILE_SUFFIX)));
        }
        return new FileId(file, context.globalProcessorIndex());
    }

    /**
     * Returns a FileResource for a fileId.
     */
    private FileResource newFileResource(FileId fileId) {
        return utility.externalGuarantee() == EXACTLY_ONCE
                ? new FileWithTransaction(fileId)
                : new FileWithoutTransaction(fileId);
    }

    private void recoverAndCommit(FileId fileId) throws IOException {
        Path tempFile = directory.resolve(fileId.fileName + TEMP_FILE_SUFFIX);
        Path finalFile = directory.resolve(fileId.fileName);
        if (Files.exists(tempFile)) {
            Files.move(tempFile, finalFile, StandardCopyOption.ATOMIC_MOVE);
        } else if (!Files.exists(finalFile)) {
            context.logger().warning("Neither temporary nor final file from the previous execution exists, data loss " +
                    "might occur: " + tempFile);
        }
    }

    private void abortUnfinishedTransactions() {
        try (Stream<Path> fileStream = Files.list(directory)) {
            fileStream
                    .filter(file -> file.getFileName().toString().endsWith(TEMP_FILE_SUFFIX))
                    .filter(file -> {
                        assert utility.usesTransactionLifecycle();
                        Matcher m = FILE_INDEX_WITH_SEQ.matcher(file.getFileName().toString());
                        if (!m.find() || m.groupCount() < 1) {
                            context.logger().warning("file with unknown name structure found in the directory: " + file);
                            return false;
                        }
                        int index;
                        try {
                            index = Integer.parseInt(m.group(1));
                        } catch (NumberFormatException e) {
                            context.logger().warning(
                                    "file with unknown name structure found in the directory: " + file, e);
                            return false;
                        }
                        // this can leave some files unhandled if the index doesn't belong
                        // to a local processor and the directory is only visible to this member.
                        // We neglect that, some abandoned tmp files will not do much harm. To
                        // fix it we would need to know whether the directory is shared or not.
                        return index % context.totalParallelism() == context.globalProcessorIndex();
                    })
                    .forEach(file -> uncheckRun(() -> {
                        LoggingUtil.logFine(context.logger(), "deleting %s",  file);
                        Files.delete(file);
                    }));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Writer createWriter(Path file) {
        try {
            LoggingUtil.logFine(context.logger(), "creating %s", file);
            FileOutputStream fos = new FileOutputStream(file.toFile(), true);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            sizeTrackingStream = new SizeTrackingStream(bos);
            return new OutputStreamWriter(sizeTrackingStream, charset);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    private String currentTimeFormatted() {
        return dateFormatter.format(Instant.ofEpochMilli(clock.getAsLong()));
    }

    public static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directoryName,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull String charset,
            @Nullable String datePattern,
            long maxFileSize,
            boolean exactlyOnce
    ) {
        return metaSupplier(directoryName, toStringFn, charset, datePattern, maxFileSize, exactlyOnce, SYSTEM_CLOCK);
    }

    // for tests
    static <T> ProcessorMetaSupplier metaSupplier(
            @Nonnull String directoryName,
            @Nonnull FunctionEx<? super T, ? extends String> toStringFn,
            @Nonnull String charset,
            @Nullable String datePattern,
            long maxFileSize,
            boolean exactlyOnce,
            @Nonnull LongSupplier clock
    ) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(ConnectorPermission.file(directoryName, ACTION_WRITE),
                SecuredFunctions.writeFileProcessorFn(directoryName, toStringFn, charset, datePattern,
                        maxFileSize, exactlyOnce, clock));
    }

    private abstract class FileResource implements TransactionalResource<FileId> {

        final FileId fileId;
        final Path targetFile;
        Writer writer;

        @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
                justification = "targetFile always has fileName")
        FileResource(FileId fileId) {
            this.fileId = fileId;
            this.targetFile = directory.resolve(fileId.fileName);
        }

        @Override
        public FileId id() {
            return fileId;
        }

        @Override
        public void endAndPrepare() throws IOException {
            closeFile();
        }

        @Override
        public void release() throws IOException {
            closeFile();
        }

        private void closeFile() throws IOException {
            if (writer != null) {
                LoggingUtil.logFine(context.logger(), "closing %s", id().fileName);
                writer.close();
                writer = null;
            }
        }

        public Writer writer() {
            return writer;
        }
    }

    private final class FileWithoutTransaction extends FileResource {

        FileWithoutTransaction(@Nonnull FileId fileId) {
            super(fileId);
        }

        @Override
        public Writer writer() {
            if (writer == null) {
                writer = createWriter(targetFile);
            }
            return super.writer();
        }
    }

    private final class FileWithTransaction extends FileResource {

        private final Path tempFile;

        FileWithTransaction(@Nonnull FileId fileId) {
            super(fileId);
            tempFile = directory.resolve(fileId.fileName + TEMP_FILE_SUFFIX);
        }

        @Override
        public void begin() {
            writer = createWriter(tempFile);
        }

        @Override
        public void commit() throws IOException {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            Files.move(tempFile, targetFile, StandardCopyOption.ATOMIC_MOVE);
        }

        @Override
        public void rollback() throws Exception {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            Files.delete(tempFile);
        }
    }

    private static final class SizeTrackingStream extends OutputStream {
        private final OutputStream target;
        private long size;

        private SizeTrackingStream(OutputStream target) {
            this.target = target;
        }

        @Override
        public void write(int b) throws IOException {
            size++;
            target.write(b);
        }

        @Override
        public void write(@Nonnull byte[] b, int off, int len) throws IOException {
            size += len;
            target.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            target.close();
        }

        @Override
        public void flush() throws IOException {
            target.flush();
        }
    }

    public static final class FileId implements TransactionId, IdentifiedDataSerializable {
        private String fileName;
        private int index;

        // for deserialization
        public FileId() {
        }

        private FileId(String fileName, int index) {
            this.fileName = fileName;
            this.index = index;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public String toString() {
            return "File{" + fileName + '}';
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.WRITE_FILE_P_FILE_ID;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            fileName = in.readUTF();
            index = in.readInt();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(fileName);
            out.writeInt(index);
        }
    }
}
