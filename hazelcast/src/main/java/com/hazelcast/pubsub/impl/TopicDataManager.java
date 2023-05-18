/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.pubsub.impl;

import com.hazelcast.internal.tpc.util.CharVector;
import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.internal.util.HashUtil;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_CRC32;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.pubsub.impl.TopicSegment.SIZEOF_SEGMENT_HEADER;

/**
 * https://www.sqlite.org/fileformat2.html#walformat
 */
public class TopicDataManager {

    private static final FileFilter LOG_FILTER = pathname -> pathname.getName().endsWith(".log");
    private static final FileFilter TOPIC_FILTER = File::isDirectory;
    private static final FileFilter PARTITION_FILTER = pathname -> {
        if (!pathname.isDirectory()) {
            return false;
        }

        try {
            Integer.parseInt(pathname.getName());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    };

    private final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());
    private final TopicPartition[] logs;
    // todo: should be configurable
    private final int segmentSize = 100 * 1024 * OS.pageSize();
    private final int maxRetainedSegments = 10;
    private final int activeBufferLength = 16 * OS.pageSize();
    private final File[] rootDirs;

    public TopicDataManager(int partitionCount) {
        this.logs = new TopicPartition[partitionCount];

        // todo: should be configurable
        this.rootDirs = new File[2];
        rootDirs[0] = new File(System.getProperty("user.home") + "/alto/drive1/topics");
        rootDirs[1] = new File(System.getProperty("user.home") + "/alto/drive2/topics");

        for (File rootDir : rootDirs) {
            logger.info("Root directory " + rootDirs);
            if (!rootDir.exists()) {
                if (!rootDir.mkdirs()) {
                    throw new UncheckedIOException(
                            new IOException("Failed to make directory [" + rootDir.getAbsolutePath() + "]"));
                }
            }
        }
    }

    public void recover() {
        logger.info("Recovering: started");

        byte[] message = new byte[segmentSize];

        for (File rootDir : rootDirs) {
            for (File topicDir : rootDir.listFiles(TOPIC_FILTER)) {
                for (File partitionDir : topicDir.listFiles(PARTITION_FILTER)) {
                    recoverPartition(message, partitionDir);
                }
            }
        }

        logger.info("Recovering: done");
    }

    // todo: this should be done on the reactor threads
    // todo: this should be done using the AsyncFile

    /**
     * Recovers the segments for some topic on some partition
     *
     * @param message
     * @param segmentDir
     */
    private void recoverPartition(byte[] message, File segmentDir) {
        logger.info("Recovering segmentDir:" + segmentDir);

        int partitionId = Integer.parseInt(segmentDir.getName());

        long activeSegmentId = -1;
        for (File segmentFile : segmentDir.listFiles(LOG_FILTER)) {
            long segmentId = recover(message, segmentFile);

            if (segmentId > activeSegmentId) {
                activeSegmentId = segmentId;
            }
        }

        if (activeSegmentId > -1) {
            logger.info("Recovering: recovered partitionId:" + partitionId + " segment:" + activeSegmentId);
            TopicPartition topicPartition = new TopicPartition();
            topicPartition.partitionId = partitionId;
            topicPartition.dir = segmentDir;
            topicPartition.segmentSize = segmentSize;
            topicPartition.maxRetainedSegments = maxRetainedSegments;
            topicPartition.activeBufferLength = activeBufferLength;
            // this will move is directly after the last segment.
            // Currently we assume that the active segment is fully written because
            // we do not track the the last written segment from the header.
            topicPartition.activeSegmentId = activeSegmentId;
            logs[partitionId] = topicPartition;
        }
    }

    private long recover(byte[] message, File segmentFile) {
        int indexOfDot = segmentFile.getName().indexOf(".");
        long segmentId = Long.parseLong(segmentFile.getName().substring(0, indexOfDot));

        if (segmentFile.length() < SIZEOF_SEGMENT_HEADER) {
            logger.info("Skipping segment due to corrupted header. Segment:" + segmentFile);
            return -1;
        }

        // TODO: File might not be the correct size.
        // TODO: We don't need to check all files.
        // TODO: We currently scan the whole file; we could check the header.
        try {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(segmentFile)))) {
                in.skip(SIZEOF_SEGMENT_HEADER);

                long pos = SIZEOF_SEGMENT_HEADER;
                int recovered = 0;
                while (true) {
                    if (in.available() < SIZEOF_INT) {
                        logger.warning("Message size incomplete."
                                + " segment:" + segmentFile.getAbsolutePath()
                                + " pos:" + pos
                                + " expected " + SIZEOF_INT
                                + " available:" + in.available());
                        break;
                    }
                    int msgLength = in.readInt();
                    if (msgLength == 0) {
                        break;
                    }

                    int readMsgBytes = in.read(message, 0, msgLength);
                    if (readMsgBytes != msgLength) {
                        logger.warning("Message incomplete."
                                + " segment:" + segmentFile.getAbsolutePath()
                                + " pos:" + pos
                                + " expected length:" + msgLength
                                + " found length:" + readMsgBytes);

                        // the message is not complete
                        break;
                    }

                    if (in.available() < SIZEOF_CRC32) {
                        logger.warning("Message crc code incomplete. "
                                + " segment:" + segmentFile.getAbsolutePath()
                                + " pos:" + pos
                                + " expected length:" + SIZEOF_CRC32
                                + " found length:" + in.available());
                        break;
                    }

                    int storedCrcCode = in.readInt();
                    int calculatedCrcCode = CRC.crc32(message, 0, msgLength);
                    if (storedCrcCode != calculatedCrcCode) {
                        logger.warning("Message crc code mismatch. "
                                + " segment:" + segmentFile.getAbsolutePath()
                                + " pos:" + pos
                                + " expected crc-code:" + calculatedCrcCode
                                + " found crc-code:" + storedCrcCode);
                        break;
                    }

                    recovered++;
                    pos += SIZEOF_INT + msgLength + SIZEOF_CRC32;
                }
                System.out.println("Last save pos:" + pos + " " + segmentFile + " recovered messages:" + recovered);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return segmentId;
    }

    public TopicPartition getOrCreateTopicPartition(CharVector name, int partitionId) {
        // todo: the name isn't used.

        TopicPartition topicPartition = logs[partitionId];
        if (topicPartition == null) {
            topicPartition = new TopicPartition();
            topicPartition.partitionId = partitionId;
            topicPartition.activeBufferLength = activeBufferLength;
            int dirIndex = HashUtil.hashToIndex(partitionId, rootDirs.length);

            File rootDir = rootDirs[dirIndex];
            File topicDir = new File(rootDir, "banana");
            File segmentDir = new File(topicDir, Integer.toString(partitionId));
            topicPartition.dir = segmentDir;
            topicPartition.segmentSize = segmentSize;
            topicPartition.activeSegmentId = 0;
            topicPartition.maxRetainedSegments = maxRetainedSegments;
            if (!topicPartition.dir.exists()) {
                // todo: handle not being able to create directory
                topicPartition.dir.mkdirs();
            }
            logs[partitionId] = topicPartition;
        }
        return topicPartition;
    }

}
