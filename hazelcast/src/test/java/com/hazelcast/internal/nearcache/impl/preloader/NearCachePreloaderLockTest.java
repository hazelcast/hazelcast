/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCachePreloaderLockTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException rule = ExpectedException.none();

    private ILogger logger = mock(ILogger.class);
    private File preloaderLockFile = new File(randomName());
    private File lockFile = new File(randomName());

    private NearCachePreloaderLock preloaderLock;
    private FileChannel channel;

    @Before
    public void setUp() throws Exception {
        preloaderLock = new NearCachePreloaderLock(logger, preloaderLockFile.getAbsolutePath());

        FileChannel realChannel = new RandomAccessFile(lockFile, "rw").getChannel();
        channel = spy(realChannel);
    }

    @After
    public void tearDown() {
        closeResource(channel);
        deleteQuietly(lockFile);
        deleteQuietly(preloaderLockFile);
    }

    /**
     * The {@link FileChannel#tryLock()} returns:
     * <pre>
     * A lock object representing the newly-acquired lock,
     * or <tt>null</tt> if the lock could not be acquired
     * because another program holds an overlapping lock
     * </pre>
     */
    @Test
    public void testAcquireLock_whenTryLockReturnsNull_thenThrowHazelcastException() throws Exception {
        when(channel.tryLock()).thenReturn(null);

        rule.expect(HazelcastException.class);
        rule.expectMessage("File is already being used by another Hazelcast instance.");
        preloaderLock.acquireLock(lockFile, channel);
    }

    /**
     * The {@link FileChannel#tryLock()} throws an {@link OverlappingFileLockException}:
     * <pre>
     * If a lock that overlaps the requested region is already held by
     * this Java virtual machine, or if another thread is already
     * blocked in this method and is attempting to lock an overlapping
     * region
     * </pre>
     */
    @Test
    public void testAcquireLock_whenTryLockThrowsOverlappingFileLockException_thenThrowHazelcastException() throws Exception {
        when(channel.tryLock()).thenThrow(new OverlappingFileLockException());

        rule.expect(HazelcastException.class);
        rule.expectMessage("File is already being used by this Hazelcast instance.");
        preloaderLock.acquireLock(lockFile, channel);
    }

    /**
     * The {@link FileChannel#tryLock()} throws an {@link IOException}:
     * <pre>
     * If some other I/O error occurs
     * </pre>
     */
    @Test
    public void testAcquireLock_whenTryLockThrowsIOException_thenThrowHazelcastException() throws Exception {
        when(channel.tryLock()).thenThrow(new IOException("expected exception"));

        rule.expect(HazelcastException.class);
        rule.expectMessage("Unknown failure while acquiring lock on " + lockFile.getAbsolutePath());
        preloaderLock.acquireLock(lockFile, channel);
    }

    @Test
    public void testRelease() {
        try {
            preloaderLock.release();
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Cannot release preloaderLock");
        }
    }
}
