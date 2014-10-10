
	

## .NET Client

![](images/enterprise-onlycopy.jpg)


You can use native .NET client to connect to Hazelcast nodes. All you need is to add `HazelcastClient3x.dll` into your .NET project references. The API is very similar to Java native client. 

.NET Client has following distributed objects:

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




Sample code is shown below.

```csharp
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
      clientConfig.GetNetworkConfig().AddAddress( "10.0.0.1" );
      clientConfig.GetNetworkConfig().AddAddress( "10.0.0.2:5702" );

      // Portable Serialization setup up for Customer Class
      clientConfig.GetSerializationConfig()
          .AddPortableFactory( MyPortableFactory.FactoryId, new MyPortableFactory() );

      IHazelcastInstance client = HazelcastClient.NewHazelcastClient( clientConfig );
      // All cluster operations that you can do with ordinary HazelcastInstance
      IMap<string, Customer> mapCustomers = client.GetMap<string, Customer>( "customers" );
      mapCustomers.Put( "1", new Customer( "Joe", "Smith" ) );
      mapCustomers.Put( "2", new Customer( "Ali", "Selam" ) );
      mapCustomers.Put( "3", new Customer( "Avi", "Noyan" ) );

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

    public IPortable Create( int classId ) {
      if ( Customer.Id == classId )
        return new Customer();
      else
        return null;
    }
  }

  public class Customer : IPortable
  {
    private string name;
    private string surname;

    public const int Id = 5;

    public Customer( string name, string surname )
    {
      this.name = name;
      this.surname = surname;
    }

    public Customer() {}

    public int GetFactoryId()
    {
      return MyPortableFactory.FactoryId;
    }

    public int GetClassId()
    {
      return Id;
    }

    public void WritePortable( IPortableWriter writer )
    {
      writer.WriteUTF( "n", name );
      writer.WriteUTF( "s", surname );
    }

    public void ReadPortable( IPortableReader reader )
    {
      name = reader.ReadUTF( "n" );
      surname = reader.ReadUTF( "s" );
    }
  }
}
```


### Client Configuration
Hazelcast .NET client can be configured via API or XML. To start the client, a configuration can be passed or can be left empty to use default values.

![image](images/NoteSmall.jpg) ***NOTE***: *.NET and Java clients are similar in terms of configuration. Therefore, you can refer to [Java Client](#java-client) section for configuration aspects. For information on .NET API documentation, please refer to the API document provided along with the Hazelcast Enterprise license*.


### Client Startup
After configuration, one can obtain a client using one of the static methods of Hazelcast like as shown below.


```csharp
IHazelcastInstance client = HazelcastClient.NewHazelcastClient(clientConfig);

...


IHazelcastInstance defaultClient = HazelcastClient.NewHazelcastClient();

...

IHazelcastInstance xmlConfClient = Hazelcast
    .NewHazelcastClient(@"..\Hazelcast.Net\Resources\hazelcast-client.xml");
```

IHazelcastInstance interface is the starting point where all distributed objects can be obtained using it.

```csharp
var map = client.GetMap<int,string>("mapName");

...

var lock= client.GetLock("thelock");
```



