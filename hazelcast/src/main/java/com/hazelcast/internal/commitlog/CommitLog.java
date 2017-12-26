package com.hazelcast.internal.commitlog;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.memory.MemoryUnit;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The CommitLog is an append only data-structure.
 */
public class CommitLog {

    private final Encoder encoder;
    private final Context context;

    // the region receiving the writes.
    private Region eden;
    // the region first in line to be evicted
    private Region oldestTenured;
    private Region youngestTenured;
    private int tenuredCount;
    private boolean frozen;
    //private final DSPartitionListeners listeners;
    private long head = 0;

    public CommitLog(Context context) {
        this.context = context;
        this.encoder = context.encoder;
    }

    /**
     * Returns the number of regions in this CommitLog.
     *
     * @return the number of regions.
     */
    public int regionCount() {
        return tenuredCount + (eden == null ? 0 : 1);
    }

    /**
     * Returns the Eden Region or null if no eden region is available.
     *
     * @return the Eden region.
     */
    public Region eden() {
        return eden;
    }


    public Region oldestTenured() {
        return oldestTenured;
    }

    public Region youngestTenured() {
        return youngestTenured;
    }

    /**
     * Returns the byte-offset of the head.
     *
     * Method is only thread safe after a Region has tenured.
     *
     * @return the byte-offset of the head.
     */
    public long head() {
        return head;
    }

    /**
     * Returns the byte-offset of the tail.
     *
     * Method is only thread safe after a Region has tenured.
     *
     * @return the byte-offset of the tail.
     */
    public long tail() {
        if (eden != null) {
            return eden.tail();
        }

        if (youngestTenured != null) {
            return youngestTenured.tail();
        }

        //todo;
        return head;
    }

//    private void loadRegionFiles() {
//        if (!storageEnabled) {
//            return;
//        }
//
//        String name = config.getName();
//        try (DirectoryStream<Path> paths = Files.newDirectoryStream(config.getStorageDir().toPath(), String.format(
//                "%02x%s-%08x-*.region", name.length(), name, partitionId))
//        ) {
//            StreamSupport.stream(paths.spliterator(), false)
//                    .sorted()
//                    .forEach(p -> newRegion(parseOffset(p)));
//        } catch (IOException e) {
//            System.out.println("WARNING: Base directory for Franz is not there: " + config.getStorageDir().getAbsolutePath());
//        }
//    }
//
//    private long parseOffset(Path p) {
//        String offsetTemplate = "0123456789ABCDEF";
//        String fileEnding = offsetTemplate + ".region";
//        String fname = p.getFileName().toString();
//        String offsetStr = fname.substring(fname.length() - fileEnding.length(), offsetTemplate.length());
//        return Long.parseLong(offsetStr, 16);
//    }

//    private CommitLogEncoder createEncoder() {
//        if (recordModel == null) {
//            HeapDataEncoder encoder = new HeapDataEncoder();
//            encoder.serializationService = serializationService;
//            return encoder;
//        }
//
//        RecordEncoderCodegen codegen = new RecordEncoderCodegen(recordModel);
//        codegen.generate();
//        Class<RecordEncoder> encoderClazz = compiler.compile(codegen.className(), codegen.getCode());
//        try {
//            Constructor<RecordEncoder> constructor = encoderClazz.getConstructor();
//            RecordEncoder encoder = constructor.newInstance();
//            encoder.setRecordModel(recordModel);
//            encoder.serializationService = serializationService;
//            return encoder;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    private void ensureEden() {
        if (eden != null && eden.tenuringAgeExceeded()) {
            return;
        }

        tenureEden();
        eden = newRegion(youngestTenured != null ? youngestTenured.tail() : head);
        tick();
    }

    private Region newRegion(long head) {
//        Map<String, Supplier<Aggregator>> attachedAggregators = config.getAttachedAggregators();
//
//        Map<String, Aggregator> aggregators;
//        if (attachedAggregators.isEmpty()) {
//            aggregators = Collections.emptyMap();
//        } else {
//            aggregators = new HashMap<>();
//            for (Map.Entry<String, Supplier<Aggregator>> entry : attachedAggregators.entrySet()) {
//                aggregators.put(entry.getKey(), entry.getValue().get());
//            }
//        }

        return new Region(head, context);
    }

    private void tenureEden() {
        if (eden == null) {
            return;
        }

        eden.tenure();
        if (oldestTenured == null) {
            // only eden exists.
            oldestTenured = eden;
            head = oldestTenured.head();
            youngestTenured = eden;
        } else {
            // there are tenured regions. So we add the eden region to the right
            // side of the tenured regions list.
            eden.previous = youngestTenured;
            youngestTenured.next = eden;
            youngestTenured = eden;
        }

        eden = null;
        tenuredCount++;
    }

    /**
     * Needs to be called periodically to move the commit log to its next state.
     * Can safely be made if there is nothing to do.
     *
     * So if eden needs to tenure or tenured regions need to be evicted.
     *
     * This method is not thread-safe.
     */
    public void tick() {
        if (eden != null && eden.tenuringAgeExceeded()) {
            tenureEden();
        }

        Region current = oldestTenured;
        while (current != null) {
            boolean maxRegionsExceeded = regionCount() > context.maxRegions;
            boolean retentionAgeExceeded = current.retentionAgeExceeded();

            if (!maxRegionsExceeded && !retentionAgeExceeded) {
                // we done, there is noting to tick.
                return;
            }

            // since we are going to expire the current region, the head if the next available
            // byte is the tail of the region to expire.
            head = current.tail();
            if (oldestTenured == youngestTenured) {
                oldestTenured = null;
                youngestTenured = null;
            } else {
                oldestTenured = current.next;
                oldestTenured.previous = null;
            }
            current.release();
            tenuredCount--;
            current = current.next;
        }
    }

    public long append(Object valueData) {
        if (frozen) {
            throw new IllegalStateException("Can't append on a frozen CommitLog");
        }

        ensureEden();
        long offset = eden.tail();
        if (!eden.append(valueData)) {
            if (eden.dataOffset() == 0) {
                throw new IllegalArgumentException("object " + valueData + " too big to be written");
            }

            tenureEden();
            ensureEden();
            offset = eden.tail();

            if (!eden.append(valueData)) {
                throw new IllegalArgumentException("object " + valueData + " too big to be written");
            }
        }

        return offset;
    }

    /**
     * Returns the record count currently stored in this CommitLog.
     *
     * @return the count.
     */
    public long count() {
        long count = 0;

        if (eden != null) {
            count += eden.count();
        }

        Region region = youngestTenured;
        while (region != null) {
            count += region.count();
            region = region.previous;
        }

        return count;
    }

    public UsageInfo usage() {
        long bytesConsumed = 0;
        long bytesAllocated = 0;
        long count = 0;
        int regionsUsed = 0;

        if (eden != null) {
            bytesConsumed += eden.bytesConsumed();
            bytesAllocated += eden.bytesAllocated();
            regionsUsed++;
            count += eden.count();
        }

        regionsUsed += tenuredCount;

        Region region = youngestTenured;
        while (region != null) {
            bytesConsumed += region.bytesConsumed();
            bytesAllocated += region.bytesAllocated();
            count += region.count();
            region = region.previous;
        }

        return new UsageInfo(bytesConsumed, bytesAllocated, regionsUsed, count);
    }

    /**
     * Freezes the CommitLog. Once frozen, no new items can be added. It also
     * tenures Eden.
     */
    public void freeze() {
        frozen = true;
        tenureEden();
        tick();
    }

    public Iterator iterator() {
        // we are not including eden for now. we rely on frozen partition
        // todo: we probably want to return youngestSegment for iteration
        // return new IteratorImpl(oldestTenured);
        throw new UnsupportedOperationException();
    }

    /**
     * Finds the region which hold data with that given offset.
     * <p>
     * Currently complexity is O(R) with R being the number of regions. We could
     * add a BST so we get complexity down to O(log(R)).
     * <p>
     * todo: as part of should there be an acquire? Because it could be that a
     * region is returned that just got frozen.
     * <p>
     * Also if a reference to a region is kept, a single slow consumer could keep
     * the chain alive.
     *
     * @param offset
     * @return
     */
    public Region findRegion(long offset) {
        Region current = oldestTenured;
        while (current != null) {
            // System.out.println("frozen: "+current.head()+" current.tail:"+current.tail());
            if (current.head() <= offset && offset < current.tail()) {
                return current;
            } else {
                current = current.next;
            }
        }

        if (eden != null) {
            // System.out.println("eden: "+eden.head()+" current.tail:"+eden.tail());
            if (eden.head() <= offset && offset < eden.tail()) {
                return eden;
            }
        }

        return null;
    }

    public void load(Load load) {
        Region region = findRegion(load.offset);
        if (region == null) {
            throw new IllegalArgumentException("No region with the given offset is found");
        }
        encoder.dataOffset = (int) (load.offset - region.head());
        long oldDataOffset = encoder.dataOffset;
        encoder.dataAddress = region.dataAddress();
        load.value = encoder.load();
        load.length = (int) (oldDataOffset - encoder.dataOffset);
    }

    public HeapData load(long offset) {
        Load load = new Load();
        load.offset = offset;
        load(load);
        return load.value;
    }

    public void loadBatch(long offset, List<HeapData> result, int batchSize) {
        throw new RuntimeException("Not yet implemented");
    }

    /**
     * Destroys this CommitLog. So all regions all being released.
     * <p>
     * After it has been destroyed, it should not be used again.
     */
    public void destroy() {
        if (eden != null) {
            eden.release();
            eden = null;
        }

        Region current = oldestTenured;
        while (current != null) {
            current.release();
            current = current.next;
        }
        oldestTenured = null;
        youngestTenured = null;
    }

//    class IteratorImpl implements Iterator {
//        private Segment region;
//        private int recordIndex = -1;
//
//        public IteratorImpl(Segment region) {
//            this.region = region;
//        }
//
//        @Override
//        public boolean hasNext() {
//            if (region == null) {
//                return false;
//            }
//
//            if (recordIndex == -1) {
//                if (!region.acquire()) {
//                    region = region.next;
//                    return hasNext();
//                } else {
//                    recordIndex = 0;
//                }
//            }
//
//            if (recordIndex >= region.count()) {
//                region.release();
//                recordIndex = -1;
//                region = region.next;
//                return hasNext();
//            }
//
//            return true;
//        }
//
//        @Override
//        public Object next() {
//            if (!hasNext()) {
//                throw new NoSuchElementException();
//            }
//
//            Object o = encoder.newInstance();
//            encoder.dataAddress = region.dataAddress();
//            encoder.dataOffset =
//            encoder.readRecord(o, region.dataAddress(), recordIndex * recordModel.getPayloadSize());
//            recordIndex++;
//            return o;
//        }
//
//        @Override
//        public void remove() {
//            throw new UnsupportedOperationException();
//        }
//    }

    public static class Context {
        File storageDir;
        Encoder encoder;
        // when set to 0, eden will not tenure based on age.
        long tenuringAgeNanos = 0;
        int initialRegionSize = 1024 * 1024;
        int maxRegionSize = 1024 * 1024;
        long maxRegions = Integer.MAX_VALUE;
        // when set to 0, regions won't be evicted based on age.
        long retentionPeriodNanos = 0;

        public long retentionPeriodNanos() {
            return retentionPeriodNanos;
        }

        public Context retentionPeriodNanos(long retentionPeriodNanos) {
            this.retentionPeriodNanos = retentionPeriodNanos;
            return this;
        }

        public Context retentionPeriod(long retentionPeriod, TimeUnit unit) {
            return retentionPeriodNanos(unit.toNanos(retentionPeriod));
        }

        public File storageDir() {
            return storageDir;
        }

        public Context storageDir(File storageDir) {
            this.storageDir = storageDir;
            return this;
        }

        public Encoder encoder() {
            return encoder;
        }

        public Context encoder(Encoder encoder) {
            this.encoder = encoder;
            return this;
        }

        public long tenuringAgeNanos() {
            return tenuringAgeNanos;
        }

        /**
         * The tenuring age; so the maximum period in nano seconds the eden region is allowed to
         * remain an eden region.
         * <p>
         * If set to zero, an eden region won't retire based on time; only when it is full, it retires.
         *
         * @param tenuringAgeNanos
         * @return
         */
        public Context tenuringAgeNanos(long tenuringAgeNanos) {
            this.tenuringAgeNanos = tenuringAgeNanos;
            return this;
        }

        public int initialRegionSize() {
            return initialRegionSize;
        }

        /**
         * Sets the initial region size in bytes.
         * <p>
         * The reason this method exists is that if you have many partitions and many streams with just
         * a few items, the overhead can be very big if a region is scaled with its max region size. So
         * in these cases is better to let the region allow to grow.
         * <p>
         * If you don't care about this, set initialRegionSize to the value of maxRegionSize.
         *
         * @param initialRegionSize
         * @return
         */
        public Context initialRegionSize(long initialRegionSize) {
            if (initialRegionSize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            if (initialRegionSize < 1) {
                throw new IllegalArgumentException();
            }
            this.initialRegionSize = (int) initialRegionSize;
            return this;
        }

        public Context initialRegionSize(long initialRegionSize, MemoryUnit unit) {
            return initialRegionSize(unit.toBytes(initialRegionSize));
        }

        public int maxRegionSize() {
            return maxRegionSize;
        }

        public Context maxRegionSize(long maxRegionSize) {
            if (maxRegionSize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            if (maxRegionSize < 1) {
                throw new IllegalArgumentException();
            }
            this.maxRegionSize = (int) maxRegionSize;
            return this;
        }

        public Context maxRegionSize(long maxRegionSize, MemoryUnit unit) {
            return maxRegionSize(unit.toBytes(maxRegionSize));
        }

        public long maxRegions() {
            return maxRegions;
        }

        public Context maxRegions(long maxRegions) {
            this.maxRegions = maxRegions;
            return this;
        }
    }

}
