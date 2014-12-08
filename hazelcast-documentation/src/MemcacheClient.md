

## Memcache Client

![image](images/NoteSmall.jpg) ***NOTE:*** *Hazelcast Memcache Client only supports ASCII protocol. Binary Protocol is not supported.*

A Memcache client written in any language can talk directly to a Hazelcast cluster. No additional configuration is required.

Assume that your cluster members are as shown below.

```plain
Members [5] {
  Member [10.20.17.1:5701]
  Member [10.20.17.2:5701]
  Member [10.20.17.4:5701]
  Member [10.20.17.3:5701]
  Member [10.20.17.5:5701]
}
```
Assume that you have a PHP application that uses PHP Memcache client to cache things in Hazelcast. All you need to do is have your PHP Memcache client connect to one of these members. It does not matter which member the client connects to because the Hazelcast cluster looks like one giant machine (Single System Image). Here is a PHP client code example.

```php
<?php
  $memcache = new Memcache;
  $memcache->connect( '10.20.17.1', 5701 ) or die ( "Could not connect" );
  $memcache->set( 'key1', 'value1', 0, 3600 );
  $get_result = $memcache->get( 'key1' ); // retrieve your data
  var_dump( $get_result ); // show it
?>
```

Notice that Memcache client connects to `10.20.17.1` and uses port`5701`. Here is a Java client code example with SpyMemcached client:

```java
MemcachedClient client = new MemcachedClient(
    AddrUtil.getAddresses( "10.20.17.1:5701 10.20.17.2:5701" ) );
client.set( "key1", 3600, "value1" );
System.out.println( client.get( "key1" ) );
```

If you want your data to be stored in different maps (e.g. to utilize per map configuration), you can do that with a map name prefix as in the following example code.


```java
MemcachedClient client = new MemcachedClient(
    AddrUtil.getAddresses( "10.20.17.1:5701 10.20.17.2:5701" ) );
client.set( "map1:key1", 3600, "value1" ); // store to *hz_memcache_map1
client.set( "map2:key1", 3600, "value1" ); // store to hz_memcache_map2
System.out.println( client.get( "key1" ) ); // get from hz_memcache_map1
System.out.println( client.get( "key2" ) ); // get from hz_memcache_map2
```

*hz\_memcache prefix\_* separates Memcache maps from Hazelcast maps. If no map name is given, it will be stored
in a default map named *hz_memcache_default*.

An entry written with a Memcache client can be read by another Memcache client written in another language.

### Unsupported Operations ###

- CAS operations are not supported. In operations that get CAS parameters, such as append, CAS values are ignored.

- Only a subset of statistics are supported. Below is the list of supported statistic values.

    - cmd_set
    -	cmd_get
    -	incr_hits
    -	incr_misses
    -	decr_hits
    -	decr_misses



<br> </br>

