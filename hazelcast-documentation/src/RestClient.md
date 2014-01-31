
## Rest Client

Let's say your cluster's members are:

```
Members [5] {
    Member [10.20.17.1:5701]
    Member [10.20.17.2:5701]
    Member [10.20.17.4:5701]
    Member [10.20.17.3:5701]
    Member [10.20.17.5:5701]
 }
```
And you have a distributed map named 'stocks'. You can put a new `key1/value1` entry into this map by issuing `HTTP POST` call to `http://10.20.17.1:5701/hazelcast/rest/maps/stocks/key1` URL. Your http post call's content body should contain the value (value1). You can retrieve this entry via `HTTP GET` call to `http://10.20.17.1:5701/hazelcast/rest/maps/stocks/key1`. You can also retrieve this entry from another member such as`http://10.20.17.3:5701/hazelcast/rest/maps/stocks/key1`.

RESTful access is provided through any member of your cluster. So you can even put an HTTP load-balancer in-front of your cluster members for load-balancing and fault-tolerance.

Now go ahead and install a REST plugin for your browser and explore further.

Hazelcast also stores the mime-type of your `POST` request if it contains any. So if, for example, you post binary of an image file and set the mime-type of the `HTTP POST` request to `image/jpeg` then this mime-type will be part of the response of your `HTTP GET` request for that entry.

Let's say you also have a task queue named 'tasks'. You can offer a new item into the queue via HTTP POST and take and item from the queue via HTTP DELETE.

`HTTP POST http://10.20.17.1:5701/hazelcast/rest/queues/tasks <CONTENT>` means

```java
Hazelcast.getQueue("tasks").offer(<CONTENT>);
```
and `HTTP DELETE http://10.20.17.1:5701/hazelcast/rest/queues/tasks/3` means

```java
Hazelcast.getQueue("tasks").poll(3, SECONDS);
```
Note that you will have to handle the failures on REST polls as there is no transactional guarantee.
