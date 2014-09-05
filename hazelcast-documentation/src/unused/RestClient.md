
## REST Client
Hazelcast provides REST interface, i.e. it provides an HTTP service in each node so that your `map` and `queue` can be accessed using HTTP protocol. Assuming `mapName` and `queueName` are already configured in your Hazelcast, its structure is shown below:

`http://node IP address:port/hazelcast/rest/maps/mapName/key`

`http://node IP address:port/hazelcast/rest/queues/queueName`

For the operations to be performed, standard REST conventions for HTTP calls are used.  

---

Assume that your cluster members are as shown below.

```
Members [5] {
    Member [10.20.17.1:5701]
    Member [10.20.17.2:5701]
    Member [10.20.17.4:5701]
    Member [10.20.17.3:5701]
    Member [10.20.17.5:5701]
 }
```


**Creating/Updating Entries in a Map**

You can put a new `key1/value1` entry into a map by using POST call to `http://10.20.17.1:5701/hazelcast/rest/maps/mapName/key1` URL. This call's content body should contain the value of the key. Also, if the call contains the MIME type, Hazelcast stores this information, too. A sample POST call is shown below.

```
$ curl -v -X POST -H "Content-Type: text/plain" -d "bar" \http://10.20.17.1:5701/hazelcast/rest/maps/mapName/foo
```

**Retrieving Entries from a Map**

If you want to retrieve an entry, you can use GET call to `http://10.20.17.1:5701/hazelcast/rest/maps/mapName/key1`. You can also retrieve this entry from another member of your cluster such as `http://10.20.17.3:5701/hazelcast/rest/maps/mapName/key1`. A sample GET call is shown below with its returns.

```
$ curl -X GET \http://10.20.17.3:5701/hazelcast/rest/maps/mapName/foo
```
```
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 3
bar
```

As you can see, GET call returned value, its length and also the MIME type (`text/plain`) since POST call sample shown above included the MIME type.

**Removing Entries from a Map**

You can use DELETE call to remove an entry. A sample DELETE call is shown below with its returns.

```
$ curl -v -X DELETE \http://10.20.17.1:5701/hazelcast/rest/maps/mapName/foo
```
```
< HTTP/1.1 204 No Content
< Content-Length: 0
```

**Offering Items on a Queue**

You can use POST call to create an item on the queue. A sample is shown below.

```
$ curl -v -X POST -H "Content-Type: text/plain" -d "foo" \http://10.20.17.1:5701/hazelcast/rest/queues/myEvents
```
Above call is equivalent to `HazelcastInstance#getQueue("myEvents").offer("foo");`.


**Retrieving Items from a Queue**

DELETE call can be used for retrieving. Note that, poll timeout should be stated while polling for queue events by an extra path parameter. A sample is shown below (**10** being the timeout value).

```
$ curl -v -X DELETE \http://10.20.17.1:5701/hazelcast/rest/queues/myEvents/10
```
Above call is equivalent to `HazelcastInstance#getQueue("myEvents").poll(10, SECONDS);`. Below is the returns of above call.

```
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 3
foo
```
When the timeout is reached, the return will be `No Content` success, i.e. there is no item on the queue to be returned.

---
Besides the above operations, you can check the status of your cluster, a sample of which is shown below.

```
$ curl -v http://127.0.0.1:5701/hazelcast/rest/cluster
```
The return will be similar to the following.

```

< HTTP/1.1 200 OK
< Content-Length: 119

Members [5] {
    Member [10.20.17.1:5701] this
    Member [10.20.17.2:5701]
    Member [10.20.17.4:5701]
    Member [10.20.17.3:5701]
    Member [10.20.17.5:5701]
 }
 
ConnectionCount: 5
AllConnectionCount: 20
```

---


RESTful access is provided through any member of your cluster. So you can even put an HTTP load-balancer in front of your cluster members for load balancing and fault tolerance.


***Note***: *You need to handle the failures on REST polls as there is no transactional guarantee.*