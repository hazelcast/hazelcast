



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
	

