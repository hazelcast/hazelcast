package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalGCStats;
import com.hazelcast.util.Clock;

import static com.hazelcast.util.JsonUtil.getLong;

public class LocalGCStatsImpl implements LocalGCStats {

    private long creationTime;
    private long minorCount;
    private long minorTime;
    private long majorCount;
    private long majorTime;
    private long unknownCount;
    private long unknownTime;

    public LocalGCStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getMajorCollectionCount() {
        return majorCount;
    }

    @Override
    public long getMajorCollectionTime() {
        return majorTime;
    }

    @Override
    public long getMinorCollectionCount() {
        return minorCount;
    }

    @Override
    public long getMinorCollectionTime() {
        return minorTime;
    }

    @Override
    public long getUnknownCollectionCount() {
        return unknownCount;
    }

    @Override
    public long getUnknownCollectionTime() {
        return unknownTime;
    }

    public void setMinorCount(long minorCount) {
        this.minorCount = minorCount;
    }

    public void setMinorTime(long minorTime) {
        this.minorTime = minorTime;
    }

    public void setMajorCount(long majorCount) {
        this.majorCount = majorCount;
    }

    public void setMajorTime(long majorTime) {
        this.majorTime = majorTime;
    }

    public void setUnknownCount(long unknownCount) {
        this.unknownCount = unknownCount;
    }

    public void setUnknownTime(long unknownTime) {
        this.unknownTime = unknownTime;
    }

//    @Override
//    public void writeData(ObjectDataOutput out)
//            throws IOException {
//        out.writeLong(creationTime);
//        out.writeLong(majorCount);
//        out.writeLong(majorTime);
//        out.writeLong(minorCount);
//        out.writeLong(minorTime);
//        out.writeLong(unknownCount);
//        out.writeLong(unknownTime);
//    }
//
//    @Override
//    public void readData(ObjectDataInput in)
//            throws IOException {
//        creationTime = in.readLong();
//        majorCount = in.readLong();
//        majorTime = in.readLong();
//        minorCount = in.readLong();
//        minorTime = in.readLong();
//        unknownCount = in.readLong();
//        unknownTime = in.readLong();
//    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("minorCount", minorCount);
        root.add("minorTime", minorTime);
        root.add("majorCount", majorCount);
        root.add("majorTime", majorTime);
        root.add("unknownCount", unknownCount);
        root.add("unknownTime", unknownTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        minorCount = getLong(json, "minorCount", -1L);
        minorTime = getLong(json, "minorTime", -1L);
        majorCount = getLong(json, "majorCount", -1L);
        majorTime = getLong(json, "majorTime", -1L);
        unknownCount = getLong(json, "unknownCount", -1L);
        unknownTime = getLong(json, "unknownTime", -1L);
    }

    @Override
    public String toString() {
        return "LocalGCStatsImpl{"
                + "creationTime=" + creationTime
                + ", minorCount=" + minorCount
                + ", minorTime=" + minorTime
                + ", majorCount=" + majorCount
                + ", majorTime=" + majorTime
                + ", unknownCount=" + unknownCount
                + ", unknownTime=" + unknownTime
                + '}';
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocalGCStatsImpl that = (LocalGCStatsImpl) o;

        if (creationTime != that.creationTime) {
            return false;
        } else if (majorCount != that.majorCount) {
            return false;
        } else if (majorTime != that.majorTime) {
            return false;
        } else if (minorCount != that.minorCount) {
            return false;
        } else if (minorTime != that.minorTime) {
            return false;
        } else if (unknownCount != that.unknownCount) {
            return false;
        } else if (unknownTime != that.unknownTime) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (minorCount ^ (minorCount >>> 32));
        result = 31 * result + (int) (minorTime ^ (minorTime >>> 32));
        result = 31 * result + (int) (majorCount ^ (majorCount >>> 32));
        result = 31 * result + (int) (majorTime ^ (majorTime >>> 32));
        result = 31 * result + (int) (unknownCount ^ (unknownCount >>> 32));
        result = 31 * result + (int) (unknownTime ^ (unknownTime >>> 32));
        return result;
    }

}
