/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

/**
 * A DirectoryLock represents a lock on a specific directory.
 * <p>
 * DirectoryLock is acquired by calling {@link #lockForDirectory(File, ILogger)}.
 */
public final class DirectoryLock {

    public static final String FILE_NAME = "lock";

    private final File dir;
    private final FileChannel channel;
    private final FileLock lock;
    private final ILogger logger;

    private DirectoryLock(File dir, FileChannel channel, FileLock lock, ILogger logger) {
        this.dir = dir;
        this.channel = channel;
        this.lock = lock;
        this.logger = logger;
    }

    /**
     * Returns the locked directory.
     */
    public File getDir() {
        return dir;
    }

    /**
     * Returns the actual {@link FileLock}.
     */
    FileLock getLock() {
        return lock;
    }

    /**
     * Releases the lock on directory.
     */
    public void release() {
        if (logger.isFineEnabled()) {
            logger.fine("Releasing lock on %s", lockFile().getAbsolutePath());
        }
        try {
            lock.release();
        } catch (ClosedChannelException e) {
            EmptyStatement.ignore(e);
        } catch (IOException e) {
            logger.severe("Problem while releasing the lock on " + lockFile(), e);
        }
        try {
            channel.close();
        } catch (IOException e) {
            logger.severe("Problem while closing the channel " + lockFile(), e);
        }
    }

    private File lockFile() {
        return new File(dir, FILE_NAME);
    }

    /**
     * Acquires a lock for given directory. A special file, named <strong>lock</strong>,
     * is created inside the directory and that file is locked.
     *
     * @param dir    the directory
     * @throws HazelcastException If lock file cannot be created, or it's already locked
     */
    public static DirectoryLock lockForDirectory(File dir, ILogger logger) {
        File lockFile = new File(dir, FILE_NAME);
        FileChannel channel = openChannel(lockFile);
        FileLock lock = acquireLock(lockFile, channel);
        if (logger.isFineEnabled()) {
            logger.fine("Acquired lock on %s", lockFile.getAbsolutePath());
        }
        return new DirectoryLock(dir, channel, lock, logger);
    }

    @SuppressWarnings("resource")
    private static FileChannel openChannel(File lockFile) {
        try {
            return new RandomAccessFile(lockFile, "rw").getChannel();
        } catch (IOException e) {
            throw new HazelcastException("Cannot create lock file " + lockFile.getAbsolutePath(), e);
        }
    }

    private static FileLock acquireLock(File lockFile, FileChannel channel) {
        FileLock fileLock = null;
        try {
            fileLock = channel.tryLock();
            if (fileLock == null) {
                throw new HazelcastException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                        + ". Directory is already being used by another member.");
            }
            return fileLock;
        } catch (OverlappingFileLockException e) {
            throw new HazelcastException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                    + ". Directory is already being used by another member.", e);
        } catch (IOException e) {
            throw new HazelcastException("Unknown failure while acquiring lock on " + lockFile.getAbsolutePath(), e);
        } finally {
            if (fileLock == null) {
                closeResource(channel);
            }
        }
    }
}
