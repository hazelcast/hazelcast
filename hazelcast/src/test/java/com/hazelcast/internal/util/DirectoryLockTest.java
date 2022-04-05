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

package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileNotFoundException;

import static com.hazelcast.internal.util.DirectoryLock.lockForDirectory;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DirectoryLockTest {

    private static final ILogger logger = Logger.getLogger(DirectoryLockTest.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File directory;
    private DirectoryLock directoryLock;

    @Before
    public void setUp() {
        directory = folder.getRoot();
    }

    @After
    public void tearDown() {
        if (directoryLock != null) {
            try {
                directoryLock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void test_lockForDirectory() {
        directoryLock = lockForDirectory(directory, logger);
        Assert.assertNotNull(directoryLock);
        Assert.assertTrue(directoryLock.getLock().isValid());
        Assert.assertEquals(directory, directoryLock.getDir());
    }

    @Test
    public void test_lockForDirectory_whenAlreadyLocked() {
        directoryLock = lockForDirectory(directory, logger);
        expectedException.expect(HazelcastException.class);
        lockForDirectory(directory, logger);
    }

    @Test
    public void test_lockForDirectory_forNonExistingDir() {
        expectedException.expect(HazelcastException.class);
        expectedException.expectCause(Matchers.<Throwable>instanceOf(FileNotFoundException.class));
        directoryLock = lockForDirectory(new File(UuidUtil.newUnsecureUuidString()), logger);
    }

    @Test
    public void test_release() {
        directoryLock = lockForDirectory(directory, logger);
        directoryLock.release();
        Assert.assertFalse(directoryLock.getLock().isValid());
    }

}
