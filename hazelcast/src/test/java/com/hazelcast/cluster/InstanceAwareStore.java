/*
 * Copyright 2013 Hazelcast, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.cluster;

import com.hazelcast.core.MapStore;
import com.hazelcast.partition.PartitionService;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author yueyulin
 */
public class InstanceAwareStore implements MapStore<String, String> {
    private String instanceId = "defaultInstanceId";
    private boolean isBackup = false;
    private String mapName = "defaultMapName";
    private String filename = null;
    PartitionService ps = null;

    public InstanceAwareStore() {
    }

    public PartitionService getPartitionService() {
        if (ps == null) {
            ps = HazelCenter.hazels.get(instanceId).getPartitionService();
        }
        return ps;
    }

    public String getFilename() {
        if (filename == null) {
            filename =  (isBackup ? "backup" : "master") + "/" + mapName;
        }
        return filename;
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public boolean isIsBackup() {
        return isBackup;
    }

    public void setIsBackup(boolean isBackup) {
        this.isBackup = isBackup;
    }

    public void store(String key, String value) {
        PartitionService ps = getPartitionService();
        int partitionId = ps.getPartition(key).getPartitionId();
        String dirName = "data/" + instanceId + "/" + partitionId + "/" + getFilename();
        File dir = new File(dirName);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        System.out.println("Store "+dir+"/"+key+".data");
        File dataFile = new File(dir, key + ".data");
        FileWriter fwriter = null;
        try {
            fwriter = new FileWriter(dataFile, false);
            fwriter.write(value);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (fwriter != null) {
                try {
                    fwriter.close();
                } catch (Exception ex) {
                }
            }
        }
    }

    public void storeAll(Map<String, String> map) {
        Iterator<String> keys = map.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            String value = map.get(key);
            store(key, value);
        }
    }

    public void delete(String key) {
        PartitionService ps = getPartitionService();
        int partitionId = ps.getPartition(key).getPartitionId();
        String dirName = "data/" + instanceId + "/" + partitionId + "/" + getFilename();
        File dir = new File(dirName);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        System.out.println("Delete File:"+dir+"/"+key+".data");
        File dataFile = new File(dir, key + ".data");
        if (dataFile.exists()) {
            dataFile.delete();
        }
    }

    public void deleteAll(Collection<String> keys) {
        for (String key : keys) {
            delete(key);
        }
    }

    public String load(String key) {
        PartitionService ps = getPartitionService();
        int partitionId = ps.getPartition(key).getPartitionId();
        String dirName = "data/" + instanceId + "/" + partitionId + "/" + getFilename();
        File dir = new File(dirName);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File dataFile = new File(dir, key + ".data");
        if(!dataFile.exists()){
            return null;
        }
        BufferedReader freader = null;
        String result = null;
        try {
            freader = new BufferedReader(new FileReader(dataFile));
            result = freader.readLine();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (freader != null) {
                try {
                    freader.close();
                } catch (Exception ex) {
                }
            }
            return result;
        }
    }

    public Map<String, String> loadAll(Collection<String> keys) {
        Map<String, String> results = new HashMap<String, String>();
        for (String key : keys) {
            String value = load(key);
            if (value == null) {
                results.put(key, value);
            }
        }
        return results;
    }

    public Set<String> loadAllKeys() {
        final Set<String> results = new HashSet<String>();
        String mapDirPath = getFilename();
        File mapDir = new File(mapDirPath);
        if (mapDir.isDirectory()) {
            File[] partitions = mapDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    if (pathname.isDirectory()) {
                        try {
                            Integer.parseInt(pathname.getName());
                        } catch (NumberFormatException nfe) {
                        }
                    }
                    return false;
                }
            });
            if (partitions != null) {
                for (File partitionDir : partitions) {
                    File[] keyFiles = partitionDir.listFiles(new FileFilter() {
                        public boolean accept(File pathname) {
                            if (pathname.isFile()) {
                                results.add(pathname.getName());
                                return true;
                            }
                            return false;
                        }
                    });
                }
            }
        }
        return results;
    }
    
}
