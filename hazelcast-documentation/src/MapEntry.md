
### Entry Statistics

Hazelcast keeps extra information about each map entry such as creation time, last update time, last access time, number of hits, version, and this information is exposed to the developer via `IMap.getEntryView(key)` call. Here is an example:

```java
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.EntryView;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
EntryView entry = hz.getMap("quotes").getEntryView("1");
System.out.println ("size in memory  : " + entry.getCost();
System.out.println ("creationTime    : " + entry.getCreationTime();
System.out.println ("expirationTime  : " + entry.getExpirationTime();
System.out.println ("number of hits  : " + entry.getHits();
System.out.println ("lastAccessedTime: " + entry.getLastAccessTime();
System.out.println ("lastUpdateTime  : " + entry.getLastUpdateTime();
System.out.println ("version         : " + entry.getVersion();
System.out.println ("key             : " + entry.getKey();
System.out.println ("value           : " + entry.getValue();
```