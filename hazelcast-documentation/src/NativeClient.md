

## Native Clients

Native Clients enable you to perform almost all Hazelcast operations without being a member of the cluster. It connects to one of the cluster members and delegates all cluster wide operations to it (*dummy client*) or connects to all of them and delegate operations smartly (*smart client*). When the relied cluster member dies, client will transparently switch to another live member.

There can be hundreds, even thousands of clients connected to the cluster. But, by default there are ***core count*** \* ***10*** threads on the server side that will handle all the requests (e.g. if the server has 4 cores, it will be 40).

Imagine a trading application where all the trading data stored and managed in a Hazelcast cluster with tens of nodes. Swing/Web applications at traders' desktops can use Native  Clients to access and modify the data in the Hazelcast cluster.

Currently, Hazelcast has Native Java, C++ and C\# Clients available.

### Java Client

You can perform almost all Hazelcast operations with Java Client. It already implements the same interface. You must include `hazelcast.jar` and `hazelcast-client.jar` into your classpath. A sample code is shown below.

```java
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.client.HazelcastClient;

import java.util.Map;
import java.util.Collection;


ClientConfig clientConfig = new ClientConfig();
clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
clientConfig.getNetworkConfig().addAddress("10.90.0.1", "10.90.0.2:5702");

HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
//All cluster operations that you can do with ordinary HazelcastInstance
Map<String, Customer> mapCustomers = client.getMap("customers");
mapCustomers.put("1", new Customer("Joe", "Smith"));
mapCustomers.put("2", new Customer("Ali", "Selam"));
mapCustomers.put("3", new Customer("Avi", "Noyan"));

Collection<Customer> colCustomers = mapCustomers.values();
for (Customer customer : colCustomers) {
     // process customer
}
```

Name and Password parameters seen above can be used to create a secure connection between the client and cluster. Same parameter values should be set at the node side, so that the client will connect to those nodes that have the same `GroupConfig` credentials, forming a separate cluster.

In the cases where the security established with `GroupConfig` is not enough and you want your clients connecting securely to the cluster, `ClientSecurityConfig` can be used. This configuration has a `credentials` parameter with which IP address and UID are set (please see [ClientSecurityConfig.java](https://github.com/hazelcast/hazelcast/blob/7133b2a84b4c97cf46f2584f1f608563a94b9e5b/hazelcast-client/src/main/java/com/hazelcast/client/config/ClientSecurityConfig.java)).

To configure the other parameters of client-cluster connection, `ClientNetworkConfig` is used. In this class, below parameters are set:

-	`addressList`: Includes the list of addresses to which the client will connect. Client uses this list to find an alive node. Although it may be enough to give only one address of a node in the cluster (since all nodes communicate with each other), it is recommended to give all nodes’ addresses.
-	`smartRouting`: This parameter determines whether the client is smart or dummy. A dummy client connects to one node specified in `addressList` and  stays connected to that node. If that node goes down, it chooses and connects another node. In the case of a dummy client, all operations that will be performed by the client are distributed to the cluster over the connected node. A smart client, on the other hand, connects to all nodes in the cluster and for example if the client will perform a “put” operation, it finds the node that is the key owner and performs that operation on that node.
-	`redoOperation`: Client may lost its connection to a cluster due to network issues or a node being down. In this case, we cannot know whether the operations that were being performed are completed or not. This boolean parameter determines if those operations will be retried or not. Setting this parameter to *true* for idempotent operations (e.g. “put” on a map) does not give a harm. But for operations that are not idempotent (e.g. “offer” on a queue), retrying them may cause undesirable effects. 
-	`connectionTimeout`: This parameter is the timeout in milliseconds for the heartbeat messages sent by the client to the cluster. If there is no response from a node for this timeout period, client deems the connection as down and closes it.
-	`connectionAttemptLimit` and `connectionAttemptPeriod`:  Assume that the client starts to connect to the cluster whose all nodes may not be up. First parameter is the count of connection attempts by the client and the second one is the time between those attempts (in milliseconds). These two parameters should be used together (if one of them is set, other should be set, too). Furthermore, assume that the client is connected to the cluster and everything was fine, but for a reason the whole cluster goes down. Then, the client will try to re-connect to the cluster using the values defined by these two parameters. If, for example, `connectionAttemptLimit` is set as *Integer.MAX_VALUE*, it will try to re-connect forever.
-	`socketInterceptorConfig`: When a connection between the client and cluster is established (i.e. a socket is opened) and if a socket interceptor is defined, this socket is handed to the interceptor. Interceptor can use this socket, for example, to log the connection or to handshake with the cluster. There are some cases where a socket interceptor should also be defined at the cluster side, for example, in the case of client-cluster handshaking. This can be used as a security feature, since the clients that do not have interceptors will not handshake with the cluster.
-	`sslConfig`: If SSL is desired to be enabled for the client-cluster connection, this parameter should be set. Once set, the connection (socket) is established out of an SSL factory defined either by a factory class name or factory implementation (please see [SSLConfig.java](https://github.com/hazelcast/hazelcast/blob/8f4072d372b33cb451e1fbb7fbd2c2489b631342/hazelcast/src/main/java/com/hazelcast/config/SSLConfig.java)).



### C++ Client (Enterprise Only)

You can use Native C++ Client to connect to Hazelcast nodes and perform almost all operations that a node can perform. Different from nodes, clients do not hold data. It is by default a smart client, i.e. it knows where the data is and asks directly to the correct node. This feature can be disabled (using `ClientConfig::setSmart` method) if you do not want the clients to connect every node.

Features of C++ Clients are:

-	Access to distributed data structures (IMap, IQueue, MultiMap, ITopic, etc.).
-	Access to transactional distributed data structures (TransactionalMap, TransactionalQueue, etc.).
-	Ability to add cluster listeners to a cluster and entry/item listeners to distributed data structures.
-	Distributed synchronization mechanisms with ILock, ISemaphore and ICountDownLatch.

Hazelcast C++ Client is shipped with 32/64 bit, shared and static libraries. Compiled static libraries of dependencies are also available in the release. Dependencies are **zlib** and some of the boost libraries. Below boost libraries are required:

-	`libboost_atomic`
-	`libboost_system`
-	`libboost_chrono`
-	`libboost_thread`
-	`libboost_date_time`

Downloaded release includes the below folders:

-	`docs/ html`, where Doxygen documentation is located.
-	`hazelcast/`
	-	`lib/`, where shared and static library of Hazelcast is located.
	-	`include/`, where client headers are included.
-	`external/`
	-	`lib/`, where compiled static libraries of dependencies is located.
	-	`include/`, where dependency headers are included.

#### Installation
C++ Client is tested on Linux 32/64, Mac 64 and Windows 32/64 bit machines.

##### Linux

For Linux, there are two distributions; 32 bit and 64 bit.

Sample script to build with static library:

`g++ main.cpp -pthread -I./external/include -I./hazelcast/include ./hazelcast/lib libHazelcastClientStatic_64.a ./external/lib/libz.a ./external/lib/libboost_thread.a ./external/lib/libboost_system.a ./external/lib/libboost_date_time.a ./external/lib/libboost_chrono.a ./external/libboost_atomic.a`

Sample script to build with shared library:

`g++ main.cpp -lpthread -Wl,–no-as-needed -lrt -I./external/include -I./hazelcast/include -L./hazelcast/lib -lHazelcastClientShared_64 ./external/lib/libz.a ./external/lib/libboost_thread.a ./external/lib/libboost_system.a ./external/lib/libboost_date_time.a ./external/lib/libboost_chrono.a ./external/lib/libboost_atomic.a`

##### Mac
For Mac, there is only one distribution which is 64 bit.

Sample script to build with static library:

`g++ main.cpp -I./external/include -I./hazelcast/include ./hazelcast/lib libHazelcastClientStatic_64.a ./external/lib/libz.a ./external/lib libboost_thread.a ./external/lib/libboost_system.a ./external/lib libboost_exception.a ./external/lib/libboost_date_time.a ./external/lib libboost_chrono.a ./external/lib/libboost_atomic.a`

Sample script to build with shared library:

`g++ main.cpp -I./external/include -I./hazelcast/include -L./hazelcast/lib -lHazelcastClientShared_64 ./external/lib/libz.a ./external/lib libboost_thread.a ./external/lib/libboost_system.a ./external/lib libboost_exception.a ./external/lib/libboost_date_time.a ./external/lib libboost_chrono.a ./external/lib/libboost_atomic.a`

##### Windows
For Windows, there are two distributions; 32 bit and 64 bit.

#### Code Examples
A Hazelcast node should be running to make below sample codes work.

##### Map Example

	 #include <hazelcast/client/HazelcastAll.h>
     #include <iostream>

     using namespace hazelcast::client;

     int main(){
         ClientConfig clientConfig;
         Address address("localhost", 5701);
         clientConfig.addAddress(address);

         HazelcastClient hazelcastClient(clientConfig);

         IMap<int,int> myMap = hazelcastClient.getMap<int ,int>("myIntMap");
         myMap.put(1,3);
         boost::shared_ptr<int> v = myMap.get(1);
         if(v.get() != NULL){
             //process the item
         }

         return 0;
     }

##### Queue Example

	 #include <hazelcast/client/HazelcastAll.h>
     #include <iostream>
     #include <string>

     using namespace hazelcast::client;

     int main(){
         ClientConfig clientConfig;
         Address address("localhost", 5701);
         clientConfig.addAddress(address);

         HazelcastClient hazelcastClient(clientConfig);

         IQueue<std::string> q = hazelcastClient.getQueue<std::string>("q");
         q.offer("sample");
         boost::shared_ptr<std::string> v = q.poll();
         if(v.get() != NULL){
             //process the item
         }
         return 0;
     }

##### Entry Listener Example

    #include "hazelcast/client/ClientConfig.h"
    #include "hazelcast/client/EntryEvent.h"
    #include "hazelcast/client/IMap.h"
    #include "hazelcast/client/Address.h"
    #include "hazelcast/client/HazelcastClient.h"
    #include <iostream>
    #include <string>

    using namespace hazelcast::client;

    class SampleEntryListener {
    public:

     void entryAdded(EntryEvent<std::string, std::string> &event) {
         std::cout << "entry added " <<  event.getKey() << " " << event.getValue() << std::endl;
     };

     void entryRemoved(EntryEvent<std::string, std::string> &event) {
         std::cout << "entry added " <<  event.getKey() << " " << event.getValue() << std::endl;
     }

     void entryUpdated(EntryEvent<std::string, std::string> &event) {
         std::cout << "entry added " <<  event.getKey() << " " << event.getValue() << std::endl;
     }

     void entryEvicted(EntryEvent<std::string, std::string> &event) {
         std::cout << "entry added " <<  event.getKey() << " " << event.getValue() << std::endl;
     }
    };


    int main(int argc, char **argv) {

     ClientConfig clientConfig;
     Address address("localhost", 5701);
     clientConfig.addAddress(address);

     HazelcastClient hazelcastClient(clientConfig);

     IMap<std::string,std::string> myMap = hazelcastClient.getMap<std::string ,std::string>("myIntMap");
     SampleEntryListener *  listener = new SampleEntryListener();

     std::string id = myMap.addEntryListener(*listener, true);
     myMap.put("key1", "value1"); //prints entryAdded
     myMap.put("key1", "value2"); //prints updated
     myMap.remove("key1"); //prints entryRemoved
     myMap.put("key2", "value2",1000); //prints entryEvicted after 1 second

     myMap.removeEntryListener(id); //WARNING: deleting listener before removing it from hazelcast leads to crashes.
     delete listener;               //delete listener after remove it from hazelcast.
     return 0;
    };  

##### Serialization Example
Assume that you have the following two classes in Java and you want to use it with C++ client. 

    class Foo implements Serializable{
        private int age;
        private String name;
    }

    class Bar implements Serializable{
        private float x;
        private float y;
    }  

**First**, let them implement `Portable` or `IdentifiedDataSerializable` as shown below.

    class Foo implements Portable {
     private int age;
     private String name;

     public int getFactoryId() {
         return 666;   // a positive id that you choose
     }

     public int getClassId() {
         return 2;     // a positive id that you choose
     }

     public void writePortable(PortableWriter writer) throws IOException {
         writer.writeUTF("n", name);
         writer.writeInt("a", age);
     }

     public void readPortable(PortableReader reader) throws IOException {
         name = reader.readUTF("n");
         age = reader.readInt("a");
     }
    }

    class Bar implements IdentifiedDataSerializable {
     private float x;
     private float y;

     public int getFactoryId() {
         return 4;     // a positive id that you choose
     }

     public int getId() {
         return 5;    // a positive id that you choose
     }

     public void writeData(ObjectDataOutput out) throws IOException {
         out.writeFloat(x);
         out.writeFloat(y);
     }

     public void readData(ObjectDataInput in) throws IOException {
         x = in.readFloat();
         y = in.readFloat();
     }
    } 

**Then**, implement the corresponding classes in C++ with same factory and class ID as shown below:

    class Foo : public Portable {
    public:
     int getFactoryId() const{
         return 666;
     };

     int getClassId() const{
         return 2;
     };

     void writePortable(serialization::PortableWriter &writer) const{
         writer.writeUTF("n", name);
         writer.writeInt("a", age);
     };

     void readPortable(serialization::PortableReader &reader){
         name = reader.readUTF("n");
         age = reader.readInt("a");
     };

    private:
     int age;
     std::string name;
    };

    class Bar : public IdentifiedDataSerializable {
     public:
         int getFactoryId() const{
             return 4;
         };

         int getClassId() const{
             return 2;
         };

         void writeData(serialization::ObjectDataOutput& out) const{
             out.writeFloat(x);
             out.writeFloat(y);
         };

         void readData(serialization::ObjectDataInput& in){
             x = in.readFloat();
             y = in.readFloat();
         };
     private:
         float x;
         float y;
     };

Now, you can use class `Foo` and `Bar` in distributed structures. For example as Key or Value of `IMap` or as an Item in `IQueue`.
	

### C# Client (Enterprise Only)

You can use native C# client to connect to Hazelcast nodes. All you need is to add `HazelcastClient3x.dll` into your C# project references. The API is very similar to Java native client. Sample code is shown below.

```
using Hazelcast.Config;
using Hazelcast.Client;
using Hazelcast.Core;
using Hazelcast.IO.Serialization;

using System.Collections.Generic;

namespace Hazelcast.Client.Example
{
    public class SimpleExample
    {

        public static void Test()
        {
            var clientConfig = new ClientConfig();
            clientConfig.GetNetworkConfig().AddAddress("10.0.0.1");
            clientConfig.GetNetworkConfig().AddAddress("10.0.0.2:5702");

            //Portable Serialization setup up for Customer CLass
            clientConfig.GetSerializationConfig().AddPortableFactory(MyPortableFactory.FactoryId, new MyPortableFactory());

            IHazelcastInstance client = HazelcastClient.NewHazelcastClient(clientConfig);
            //All cluster operations that you can do with ordinary HazelcastInstance
            IMap<string, Customer> mapCustomers = client.GetMap<string, Customer>("customers");
            mapCustomers.Put("1", new Customer("Joe", "Smith"));
            mapCustomers.Put("2", new Customer("Ali", "Selam"));
            mapCustomers.Put("3", new Customer("Avi", "Noyan"));

            ICollection<Customer> customers = mapCustomers.Values();
            foreach (var customer in customers)
            {
                //process customer
            }
        }
    }

    public class MyPortableFactory : IPortableFactory
    {
        public const int FactoryId = 1;

        public IPortable Create(int classId) {
            if (Customer.Id == classId)
                return new Customer();
            else return null;
        }
    }

    public class Customer: IPortable
    {
        private string name;
        private string surname;

        public const int Id = 5;

        public Customer(string name, string surname)
        {
            this.name = name;
            this.surname = surname;
        }

        public Customer(){}

        public int GetFactoryId()
        {
            return MyPortableFactory.FactoryId;
        }

        public int GetClassId()
        {
            return Id;
        }

        public void WritePortable(IPortableWriter writer)
        {
            writer.WriteUTF("n", name);
            writer.WriteUTF("s", surname);
        }

        public void ReadPortable(IPortableReader reader)
        {
            name = reader.ReadUTF("n");
            surname = reader.ReadUTF("s");
        }
    }
}
```


#### Client Configuration
Hazelcast C# client can be configured via API or XML. To start the client, a configuration can be passed or can be left empty to use default values.

***Note***: *C# and Java clients are similar in terms of configuration. Therefore, you can refer to [Java Client](#java-client) section for configuration aspects. For information on C# API documentation, please refer to ???*.


#### Client Startup
After configuration, one can obtain a client using one of the static methods of Hazelcast like as shown below.


```
IHazelcastInstance client = HazelcastClient.NewHazelcastClient(clientConfig);

...


IHazelcastInstance defaultClient = HazelcastClient.NewHazelcastClient();

...

IHazelcastInstance xmlConfClient = Hazelcast.NewHazelcastClient(@"..\Hazelcast.Net\Resources\hazelcast-client.xml");
```

IHazelcastInstance interface is the starting point where all distributed objects can be obtained using it.

```
var map = client.GetMap<int,string>("mapName");

...

var lock= client.GetLock("thelock");
```

C# Client has following distributed objects:

* `IMap<K,V>`
* `IMultiMap<K,V>`
* `IQueue<E>`
* `ITopic<E>`
* `IHList<E>`
* `IHSet<E>`
* `IIdGenerator`
* `ILock`
* `ISemaphore`
* `ICountDownLatch`
* `IAtomicLong`
* `ITransactionContext`
	
	ITransactionContext can be used to obtain;

	* `ITransactionalMap<K,V>`
	* `ITransactionalMultiMap<K,V>`
	* `ITransactionalList<E>`
	* `ITransactionalSet<E>`

