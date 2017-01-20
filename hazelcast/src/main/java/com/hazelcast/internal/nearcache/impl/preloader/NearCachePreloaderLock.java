/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.preloader;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import static com.hazelcast.nio.IOUtil.closeResource;

class NearCachePreloaderLock {

    private final ILogger logger;

    private final File lockFile;
    private final FileChannel channel;
    private final FileLock lock;

    NearCachePreloaderLock(ILogger logger, String lockFileName) {
        this.logger = logger;

        this.lockFile = new File(lockFileName);
        this.channel = openChannel(lockFile);
        this.lock = acquireLock(lockFile);
    }

    private FileChannel openChannel(File lockFile) {
        try {
            return new RandomAccessFile(lockFile, "rw").getChannel();
        } catch (IOException e) {
            throw new HazelcastException("Cannot create lock file " + lockFile.getAbsolutePath(), e);
        }
    }

    private FileLock acquireLock(File lockFile) {
        FileLock fileLock = null;
        try {
            fileLock = channel.tryLock();
            if (fileLock != null) {
                return fileLock;
            }
            throw new HazelcastException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                    + ". File is already being used by another Hazelcast instance.");
        } catch (OverlappingFileLockException e) {
            throw new HazelcastException("Cannot acquire lock on " + lockFile.getAbsolutePath()
                    + ". File is already being used by this Hazelcast instance.", e);
        } catch (IOException e) {
            throw new HazelcastException("Unknown failure while acquiring lock on " + lockFile.getAbsolutePath(), e);
        } finally {
            if (fileLock == null) {
                closeResource(channel);
            }
        }
    }

    void release() {
        try {
            lock.release();
            channel.close();
        } catch (IOException e) {
            logger.severe("Problem while releasing the lock and closing channel on " + lockFile, e);
        } finally {
            lockFile.deleteOnExit();
        }
    }
}
