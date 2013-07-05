package com.hazelcast.cluster;

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
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.impl.ConcurrentMapManager;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;

/**
 *
 * @author yueyulin
 */
public class BackupStoreTest {

    private HazelcastInstance[] hs;
    private String[] ids = new String[5];

    public BackupStoreTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    private Config createConfig(String instanceId) {
        Config c = new Config();
        Map<String, MapConfig> mapConfigs = new HashMap<String, MapConfig>();
        mapConfigs.put("default", createMapConfig(instanceId));
        c.setMapConfigs(mapConfigs);
        return c;
    }

    private MapConfig createMapConfig(String instanceId) {
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig storeConfig = new MapStoreConfig();
        storeConfig.setFactoryClassName("com.hazelcast.cluster.InstanceAwareStoreFactory");
        storeConfig.setBackupFactoryClassName("com.hazelcast.cluster.BackupInstanceAwareStoreFactory");
        Properties props = new Properties();
        props.setProperty("instanceId", instanceId);
        storeConfig.setProperties(props);
        storeConfig.setEnabled(true);
        storeConfig.setWriteDelaySeconds(1);
        mapConfig.setMapStoreConfig(storeConfig);
        mapConfig.setBackupCount(0);
        mapConfig.setAsyncBackupCount(1);
        return mapConfig;
    }

    @Before
    public void setUp() {
        Config[] configs = new Config[5];
        hs = new HazelcastInstance[5];
        System.out.println("Start to startup hazelcast instances");
        for (int i = 0; i < ids.length; i++) {
            ids[i] = "id" + i;
            configs[i] = createConfig(ids[i]);
            System.err.println(configs[i]);
            hs[i] = Hazelcast.newHazelcastInstance(configs[i]);
            HazelCenter.hazels.put(ids[i], hs[i]);
        }
        File dataDir = new File("data");
        dataDir.mkdir();
    }

    public String getId(Address addr) {
        int i = 0;
        for (; i < 5; i++) {
            if (hs[i].getCluster().getLocalMember().getInetSocketAddress().getPort()
                    == addr.getPort()) {
                break;
            }
        }
        if (i == 5) {
            throw new RuntimeException("No match address!");
        }
        return "id" + i;
    }

    public String readKeyFile(String file) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String result = br.readLine().trim();
            return result;
        } catch (Exception ex) {
            Logger.getLogger(BackupStoreTest.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                br.close();
            } catch (Exception ex) {
            }
        }
        return null;
    }

    @Test
    public void testPutDelete() {
        int dataSize = 10000;
        String mapName = "mybabe";
        System.out.println("Start to put data");
        for (int i = 0; i < dataSize; i++) {
            IMap<String, String> map = hs[i % 5].getMap(mapName);
            String key = "key" + i;
            String value = Double.toString(new Random().nextDouble());
            map.put(key, value);
        }
        try {
            Thread.sleep(60000);
        } catch (InterruptedException ex) {
            Logger.getLogger(BackupStoreTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        //Data dir
        String dataDir = "data/";
        ClusterImpl cluster = (ClusterImpl) hs[0].getCluster();
        ConcurrentMapManager cm = cluster.node.concurrentMapManager;

        //Start to verify if the data has come to the right place
        for (int i = 0; i < dataSize; i++) {
            String key = "key" + i;
            System.out.println(key);

            int partitionId = hs[0].getPartitionService().getPartition(key).getPartitionId();
            PartitionInfo pi = cm.getPartitionInfo(partitionId);
            Address myself = pi.getReplicaAddress(0);
            String id = getId(myself);
            String masterDataDir = dataDir + id + "/" + partitionId + "/master/";
            //String backupDataDir = dataDir+backupId+"/"+partitionId+"/backup/";
            System.out.println(masterDataDir);
            //System.out.println(backupDataDir);
            File masterDir = new File(masterDataDir);
            //File backupDir = new File(backupDataDir);
            //Assert if these two directory exists
            assertTrue(masterDir.isDirectory());
            int countBackup = 0;
            String backupDataDir = null;
            for (int j = 0; j < 5; j++) {
                String bkDir = dataDir + "id" + j + "/" + partitionId + "/backup/";
                File backupDir = new File(bkDir);
                if (backupDir.isDirectory()) {
                    backupDataDir = bkDir;
                    System.out.println(backupDataDir);
                    countBackup++;
                }
            }
            assertEquals(1, countBackup);

            System.out.println("Compare the value between primary and backup");
            String primaryDataFile = masterDir + "/" + mapName + "/" + key + ".data";
            String backupDataFile = backupDataDir + "/" + mapName + "/" + key + ".data";
            File primaryFile = new File(primaryDataFile);
            File backupFile = new File(backupDataFile);
            System.out.println("check key file:" + primaryDataFile);
            assertTrue(primaryFile.isFile());
            System.out.println("check backup key file:" + backupDataFile);
            assertTrue(backupFile.isFile());
            //Start to compare file content
            System.out.println("check file content");
            String primaryContent = readKeyFile(primaryDataFile);
            String backupContent = readKeyFile(backupDataFile);
            assertEquals(primaryContent, backupContent);
            System.out.println("************************");


        }



        System.out.println("Start to delete data");
        for (int i = 0; i < dataSize; i++) {
            IMap<String, String> map = hs[i % 5].getMap(mapName);
            String key = "key" + i;
            map.remove(key);
        }
        try {
            Thread.sleep(60000);
        } catch (InterruptedException ex) {
            Logger.getLogger(BackupStoreTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        //Check if all data is erased including primary and backup 
        for (int i = 0; i < 5; i++) {
            String id = "id" + i;
            String idPrimaryDir = "data/" + id;
            File primaryDir = new File(idPrimaryDir);
            //List partitions in the data directory
            File[] partitionDirs = primaryDir.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    if (pathname.isFile()) {
                        return false;
                    }
                    String name = pathname.getName();
                    try {
                        Integer.parseInt(name);
                    } catch (Exception ex) {
                        return false;
                    }
                    return true;
                }
            });
            if (partitionDirs != null) {
                for (File partitionDir : partitionDirs) {
                    File masterDir = new File(partitionDir, "master");
                    File backupDir = new File(partitionDir, "backup");
                    assertFalse((masterDir.isDirectory() && backupDir.isDirectory()));
                    File realDir = new File(masterDir.isDirectory() ? masterDir : backupDir, mapName);
                    System.out.println("Scan:" + realDir);
                    File[] datafiles = realDir.listFiles(new FileFilter() {
                        public boolean accept(File pathname) {
                            if (pathname.isFile()) {
                                return true;
                            }
                            return false;
                        }
                    });
                    boolean isDataFilesExits = (datafiles == null) || (datafiles.length == 0);
                    assertTrue(isDataFilesExits);
                }
            }
        }
    }

    static public boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                } else {
                    files[i].delete();
                }
            }
        }
        return (path.delete());
    }

    @After
    public void tearDown() {
        for (HazelcastInstance hz : hs) {
            hz.getLifecycleService().kill();
        }
        File dataDir = new File("data");
        deleteDirectory(dataDir);
        HazelCenter.hazels.clear();
    }
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
