



## REST Client
Hazelcast provides a REST interface, i.e. it provides an HTTP service in each node so that your `map` and `queue` can be accessed using HTTP protocol. Assuming `mapName` and `queueName` are already configured in your Hazelcast, its structure is shown below.

`http://node IP address:port/hazelcast/rest/maps/mapName/key`

`http://node IP address:port/hazelcast/rest/queues/queueName`

For the operations to be performed, standard REST conventions for HTTP calls are used.



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

---

![image](images/NoteSmall.jpg) ***NOTE***: *All of the requests below can return one of the following responses in case of a failure*

- If the HTTP request syntax is not known, the following response will be returned.

```plain
HTTP/1.1 400 Bad Request
Content-Length: 0
```


- In case of an unexpected exception, the following response will be returned.

```plain
< HTTP/1.1 500 Internal Server Error
< Content-Length: 0
```

---


**Creating/Updating Entries in a Map**

You can put a new `key1/value1` entry into a map by using POST call to 
`http://10.20.17.1:5701/hazelcast/rest/maps/mapName/key1` URL. This call's content body should contain the value of the key. Also, if the call contains the MIME type, Hazelcast stores this information, too. 

A sample POST call is shown below.

```plain
$ curl -v -X POST -H "Content-Type: text/plain" -d "bar" 
    http://10.20.17.1:5701/hazelcast/rest/maps/mapName/foo
```

It will return the following response if successful:

```plain
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 0
```

**Retrieving Entries from a Map**

If you want to retrieve an entry, you can use a GET call to `http://10.20.17.1:5701/hazelcast/rest/maps/mapName/key1`. You can also retrieve this entry from another member of your cluster, such as 
`http://10.20.17.3:5701/hazelcast/rest/maps/mapName/key1`.

An example of a GET call is shown below.

```plain
$ curl -X GET http://10.20.17.3:5701/hazelcast/rest/maps/mapName/foo
```

It will return the following response if there is a corresponding value:

```plain
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 3
bar
```

This GET call returned a value, its length, and also the MIME type (`text/plain`) since the POST call example shown above included the MIME type.

It will return the following if there is no mapping for the given key:

```plain
< HTTP/1.1 204 No Content
< Content-Length: 0
```


**Removing Entries from a Map**

You can use a DELETE call to remove an entry. A sample DELETE call is shown below with its response.

```plain
$ curl -v -X DELETE http://10.20.17.1:5701/hazelcast/rest/maps/mapName/foo
```
```
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 0
```
If you leave the key empty as follows, DELETE will delete all entries from the map.

```plain
$ curl -v -X DELETE http://10.20.17.1:5701/hazelcast/rest/maps/mapName
```

```plain
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 0
```

**Offering Items on a Queue**

You can use a POST call to create an item on the queue. A sample is shown below.

```plain
$ curl -v -X POST -H "Content-Type: text/plain" -d "foo" 
    http://10.20.17.1:5701/hazelcast/rest/queues/myEvents
```

The above call is equivalent to `HazelcastInstance#getQueue("myEvents").offer("foo");`.

It will return the following if successful:

```plain
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 0
```

It will return the following if the queue is full and the item is not able to be offered to the queue:

```plain
< HTTP/1.1 503 Service Unavailable
< Content-Length: 0
```

**Retrieving Items from a Queue**

You can use a DELETE call for retrieving items from a queue. Note that you should state the poll timeout while polling for queue events by an extra path parameter. 

An example is shown below (**10** being the timeout value).

```plain
$ curl -v -X DELETE \http://10.20.17.1:5701/hazelcast/rest/queues/myEvents/10
```

The above call is equivalent to `HazelcastInstance#getQueue("myEvents").poll(10, SECONDS);`. Below is the response.

```plain
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 3
foo
```

When the timeout is reached, the response will be `No Content` success, i.e. there is no item on the queue to be returned.


```plain
< HTTP/1.1 204 No Content
< Content-Length: 0
```


**Getting the size of the queue**

```plain
$ curl -v -X GET \http://10.20.17.1:5701/hazelcast/rest/queues/myEvents/size
```

The above call is equivalent to `HazelcastInstance#getQueue("myEvents").size();`. Below is a sample response.

```plain
< HTTP/1.1 200 OK
< Content-Type: text/plain
< Content-Length: 1
5
```
---
Besides the above operations, you can check the status of your cluster, a sample of which is shown below.

```plain
$ curl -v http://127.0.0.1:5701/hazelcast/rest/cluster
```

The return will be similar to the following:

```plain
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

RESTful access is provided through any member of your cluster. You can even put an HTTP load-balancer in front of your cluster members for load balancing and fault tolerance.


![image](images/NoteSmall.jpg) ***NOTE***: *You need to handle the failures on REST polls as there is no transactional guarantee.*


