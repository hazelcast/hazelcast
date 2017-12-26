/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this regionFile except in compliance with the License.
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

package com.hazelcast.internal.commitlog;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.lang.System.nanoTime;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

/**
 * Todo:
 * One of the requirement should be that if you a previous region acquires the next region so that
 * a next region can't thrown away until the previous region has been thrown away. Otherwise we can
 * run into gaps while iterating over the chain of regions. You could have a reference to an old region
 * and when you jump to the younger one in front of it, you find out that this younger region already
 * has its memory freed.
 *  
 * So probably when we link 2 regions, the older region should do 1 acquire on the younger region and
 * when the older region is deleted, then it will call a release on the younger region.
 */
public class Region {

    private final static AtomicIntegerFieldUpdater OWNERS = newUpdater(Region.class, "owners");

    // both previous and next region can be read by an arbitrary thread, but will only be
    // updates by the thread owning the partition.
    // points to the next region (this region is younger)
    // once set, will never be unset.
    // so if you get a reference to a region, you can always move to a younger region, but it could be
    // that the memory of these younger regions is released.
    public volatile Region next;
    // points to the previous region (this region is older).
    // once set, this field will be unset when Region is evicted.
    public volatile Region previous;

    // Provides 'smart pointer' like behavior so the region can be shared between threads once it is tenured.
    // A region is 'immutable' from that point on, but its resources need to be released once nobody is referring
    // to the region. Using this mechanism we can safely share regions between threads.
    // we start with 1 because the partition is using the eden region and doesn't need to acquire.
    private volatile int owners = 1;

    private final CommitLog.Context context;
    private final Encoder encoder;
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final long bornNanos = nanoTime();
    private final long indicesAddress;
    //   private final Map<String, Aggregator> aggregators;
    private final long head;

    private long lastAppendNanos = bornNanos;

    // the pointer to the chunk of memory containing records
    private long dataAddress;
    // the length in bytes of the chunk of memory containing the records.
    private int dataLength;
    // the offset of the first available byte relative to the dataAddress.
    private int dataOffset;
    // the number of records stored in this Region.
    private int recordCount;
    private volatile boolean tenured;

    Region(long head, CommitLog.Context context) {
        this.head = head;
        this.context = context;
        this.indicesAddress = 0;
        this.dataLength = context.initialRegionSize;
        this.dataAddress = unsafe.allocateMemory(dataLength);
        this.encoder = context.encoder;
    }

    /**
     * Tenures this region.
     *  
     * Once tenured, this region can never be 'untenured'.
     */
    public void tenure() {
        tenured = true;
    }

    /**
     * Returns the byte offset of the first byte in this Region.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return the head.
     */
    public long head() {
        return head;
    }

    /**
     * Returns the byte offset of the tail (so the side where data gets written too).
     *  
     * If head == tail, then the partition is empty.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return the tail.
     */
    public long tail() {
        return head + dataOffset;
    }

    /**
     * Returns the number of records in this region.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *  
     * Method can only be called from the
     *
     * @return the count.
     */
    public long count() {
        return recordCount;
    }

    /**
     * Returns the address to the indices.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return the address to the indices or 0 if there is no index.
     */
    public long indicesAddress() {
        return indicesAddress;
    }

    /**
     * Returns the address to the data.
     *  
     * While this region is an eden region, this value can change when the region grows.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return the data address
     */
    public long dataAddress() {
        return dataAddress;
    }

    /**
     * Returns the offset of the data within this Region. So it needs to be combined
     * with the data address to get a valid pointer to a record.
     *
     * @return the data offset.
     */
    public int dataOffset() {
        return dataOffset;
    }

    /**
     * Returns the first time an append was done on this region.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return the first time an append was done
     */
    public long bornNanos() {
        return bornNanos;
    }

    /**
     * Returns the last time an append was done on this region.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return the last time in nanos a write was done.
     */
    public long lastAppendNanos() {
        return lastAppendNanos;
    }

    /**
     * Checks if this Region should tenure based on its age. The age is the duration between the time this region
     * was born and System.nanoTime.
     *  
     * If {@link com.hazelcast.internal.commitlog.CommitLog.Context#tenuringAgeNanos} is 0, then tenuring
     * age will never be exceeded.
     *
     * @return true if tenuring age is exceeded.
     */
    boolean tenuringAgeExceeded() {
        if (context.tenuringAgeNanos == 0) {
            return false;
        }

        long ageNanos = System.nanoTime() - bornNanos;
        return ageNanos - context.tenuringAgeNanos > 0;
    }

    /**
     * Checks if this Region retention age is exceeded. The age is the between System.nanoTime and the
     * this Region was born.
     *  
     * If {@link com.hazelcast.internal.commitlog.CommitLog.Context#retentionPeriodNanos} is 0, then
     * the retention age will never be exceeded.
     *
     * @return
     */
    boolean retentionAgeExceeded() {
        if (context.retentionPeriodNanos == 0) {
            return false;
        }

        long ageNanos = System.nanoTime() - bornNanos;
        return ageNanos - context.retentionPeriodNanos > 0;
    }

    /**
     * Returns the number of bytes allocated for this Region.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return bytes allocated
     */
    public long bytesAllocated() {
        // todo: doesn't include indices
        return dataLength;
    }

    /**
     * Returns the number of bytes consumed in this Region. In most cases the bytes consumed will be smaller than
     * the bytes allocated.
     *  
     * Once the region is tenured, this field can be accessed by multiple threads because the region is immutable.
     *
     * @return bytes allocated
     */
    public long bytesConsumed() {
        // todo: the indices are not included
        return dataOffset;
    }

    /**
     * Acquires this region. This is needed before the Region is used.
     * 
     * This method is thread-safe.
     *
     * @return true if this region was acquired successfully. If false is returned, this region can never
     * be acquired again.
     * @see #release()
     */
    public boolean acquire() {
        for (; ; ) {
            int oldOwners = owners;
            if (oldOwners == 0) {
                return false;
            }
            int newOwners = oldOwners + 1;
            if (OWNERS.compareAndSet(this, oldOwners, newOwners)) {
                return true;
            }
        }
    }

    /**
     * Releases the Region.
     *  
     * The Region makes use of a smart-pointer to allow for a region to be used by multiple threads.
     *  
     * Once the Region has 0 owners, it can never be acquired again.
     *  
     * This method is thread-safe.
     * @see #acquire()
     */
    public void release() {
        for (; ; ) {
            int oldOwners = owners;
            int newOwners = oldOwners - 1;
            if (OWNERS.compareAndSet(this, oldOwners, newOwners)) {
                if (newOwners == 0) {
                    // we are the last one using this region, so we can write to file
                    // and release the memory.
                    //saveToFile();
                    unsafe.freeMemory(dataAddress);
                    if (indicesAddress != 0) {
                        unsafe.freeMemory(indicesAddress);
                    }
                }
                return;
            }
        }
    }

    /**
     * Appends the record to this region.
     *  
     * Appending is pretty simple; just a matter of bump the pointer since this is an append only
     * data-structure.
     *  
     * This method can only be called on an eden region and only by the thread owning the partition.
     *
     * @param record the record to append.
     * @return true if the record got appended, false if there was not space to append the record.
     */
    public boolean append(Object record) {
        if (tenured) {
            throw new IllegalArgumentException("Can't append to a tenured region");
        }

        for (; ; ) {
            encoder.dataAddress = dataAddress;
            encoder.dataOffset = dataOffset;
            encoder.dataLength = dataLength;
            encoder.indicesAddress = indicesAddress;
            if (encoder.store(record)) {
                // we managed to write, so we are done.
                dataOffset = encoder.dataOffset;
                recordCount++;
                lastAppendNanos = nanoTime();
                return true;
            }

            // we didn't manage to write, lets try to grow the region
            int newDataLength = Math.min(context.maxRegionSize, 2 * this.dataLength);
            if (newDataLength == dataLength) {
                // we didn't manage to grow the region; so we are done.
                return false;
            }

            System.out.println("Growing region from:" + this.dataLength + " to:" + newDataLength);
            long newDataAddress = unsafe.allocateMemory(newDataLength);
            unsafe.copyMemory(dataAddress, newDataAddress, dataLength);
            unsafe.freeMemory(dataAddress);
            dataLength = newDataLength;
            dataAddress = newDataAddress;
        }
    }
//
//    private File newRegionFile(String name, int partitionId, long startOffset) {
//        if (!config.isStorageEnabled()) {
//            return null;
//        }
//
//        return new File(config.getStorageDir(),
//                String.format("%02x%s-%08x-%016x.region", name.length(), name, partitionId, startOffset));
//    }

//    /**
//     * Loads the region from file.
//     *
//     * @return true if loaded, false otherwise.
//     */
//    private boolean loadFromFile() {
//        if (!config.isStorageEnabled()) {
//            return false;
//        }
//
//        long reportedFileSize = regionFile.length();
//        long newPointer = unsafe.allocateMemory(reportedFileSize);
//        long totalRead = 0;
//        try (InputStream in = new FileInputStream(regionFile)) {
//            byte[] buf = new byte[1 << 14];
//            for (int readCount = 0; readCount != -1; readCount = in.read(buf)) {
//                if (totalRead + readCount > reportedFileSize) {
//                    throw new HazelcastException(String.format(
//                            "%s: reported file size was %,3d, but more data was there, at least %,3d bytes",
//                            regionFile, reportedFileSize, totalRead + readCount));
//                }
//                MEM.copyFromByteArray(buf, 0, newPointer + totalRead, readCount);
//                totalRead += readCount;
//            }
//            return true;
//        } catch (FileNotFoundException e) {
//            return false;
//        } catch (IOException e) {
//            throw new HazelcastException("Failed to load data from file " + regionFile, e);
//        }
//    }

//    private void saveToFile() {
//        if (!config.isStorageEnabled()) {
//            return;
//        }
//
//        try (OutputStream out = new FileOutputStream(regionFile)) {
//            byte[] buf = new byte[1 << 14];
//            long fileSize = dataOffset;
//            for (long offset = 0; offset < fileSize; offset += buf.length) {
//                int batchSize = Math.min(buf.length, (int) (fileSize - offset));
//                MEM.copyToByteArray(dataAddress + offset, buf, 0, batchSize);
//                out.write(buf, 0, batchSize);
//            }
//        } catch (IOException e) {
//            throw new HazelcastException("Failed to save data to file " + regionFile, e);
//        }
//    }
}
