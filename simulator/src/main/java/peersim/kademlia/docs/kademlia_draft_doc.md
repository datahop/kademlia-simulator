
## **Kademlia Protocol Documentation**

### **1. Summary**

This class provides a simulation environment for the Kademlia protocol using PeerSim, an event-driven simulator for P2P systems. The Kademlia protocol can be used to build a DHT that enables nodes to either locate other nodes or locate and retrieve data based on keys.

### **2. Overview**

The Kademlia Simulator is a peer to peer network simulation program that uses the KademliaProtocol class as its core implementation of the Kademlia protocol. This class spans approximately 700 lines of code and incorporates various features such as a routing table data structure to monitor nodes within the network, functions to include new nodes, locate nodes in the network, and manage events. The protocol also defines different message types, such as Find, Put, Get, and Store, which nodes leverage to execute different operations like finding the nearest nodes to a specific key, saving data within the network, and responding to messages.

In order handle send and receive messages/events, the KademliaProtocol class provides the **`sendMessage`** method and the **`handleResponse`** methods. The former sends messages between nodes in the network, while the latter handles incoming events, such as responses to messages, put requests, get requests, and so on. The class also includes several helper classes like **`FindOperation`**, which handles find node operations, and Timeout (currently not in use??), which manages timeout events.

Apart from the KademliaProtocol class many other classes are used, but two are important for starting a simulation are avaialble, CustomDistribution and StateBuilder. They are implemented to provide network **`initialization`** and bootstrap functionality. At the beginning of the network initialization, the WireKOut class builds randomly the interconnections between nodes to create a virtual network overlay. Then, the CustomDistribution class assigns a unique ID to every node to initialize the whole network. Finally, the StateBuilder class performs the node’s bootstrap process filling the k-buckets of all initial nodes. In particular, the initial bootstrap process has been implemented by adding to each peer the IDs of 100 (50 is stated in the comments, but this seems wrong) random nodes and its 50 nearest nodes, thus filling the various routing tables in a simple manner, granting to every peer the knowledge of its neighbors and some other random nodes, distributed among the network (Daniele Furlan and Maurizio Bonani, 2009/2010).

### **3. Class and Method Description**

Before adopting Javadoc, the following descriptions were written for the classes and methods in the **`KademliaProtocol`** Class. The javadoc generated documentation can be found in the apidocs subfolder within the docs folder [here](../docs/apidocs/) (or following ../docs/apidocs/). 

The primary/main class is the **`KademliaProtocol`** class, which implements the **`EDProtocol`** interface. It has several instance variables, including `nodeId`, which represents the ID of the node running the protocol; `routingTable`, which is the routing table for the node; and `sentMsg` and `findOp`, which are data structures used to keep track of messages sent and find operations, respectively.

The class also has several instance variables that store various parameters and objects needed for the implementation of the protocol. Some of the variables are used as parameter names in the PeerSim configuration file. Notable variables are:

- `PAR_K`: serves as the parameter name for K. This specifies the number of nodes stored in each bucket of the routing table.
- `PAR_ALPHA`: serves as the parameter name for $\alpha$. This specifies the number of parallel messages that can be sent during a lookup operation.
- `PAR_BITS`: The parameter name for BITS. This specifies the length of the node IDs in bits.
- `PAR_FINDMODE`: serves as the parameter name for FINDMODE. This specifies the mode of the find operation (not sure about this - confirm).
- `PAR_TRANSPORT`: used to set the transport layer for the protocol.

The class also has **`clone`** and **`init`** methods for creating and initializing KademliaProtocol objects. The **`clone`** method creates a new instance of the **`KademliaProtocol`** class that is identical to the original while the **`KademliaProtocol`** constructor initializes several fields of the **`KademliaProtocol`** object, including its `nodeId`, `routingTable`, `sentMsg`, `findOp`, and `tid`. Then, the **`_init`** method is called to set the Kademlia configuration values such as `K`, `ALPHA`, and `BITS` that are shared by all nodes in the network. It also checks if `_ALREADY_INSTALLED` is true to prevent the method from being called more than once. Finally, the `routingTable`, `sentMsg`, and `findOp` fields are initialized with new objects of the **`RoutingTable`**, **`TreeMap<Long, Long>`**, and **`LinkedHashMap<Long, FindOperation>`** classes, respectively.

For completeness, we digress a bit and give the details of the `RoutingTable` class
- ## **Digression - RoutingTable Class**
    ### **Description** 
    The class provides a data structure for efficiently storing and retrieving nodes in a Kademlia DHT network. It has three instance variables:
    - `nodeId`: BigInteger representing the ID of the node;
    - `k_buckets`: TreeMap that stores the k-buckets of the node. A k-bucket is a list of nodes that  share the same prefix length with the node;
        - **Another Digression - Details of the `KBucket` implementation**
            The **`KBucket`** class is data structure used in the Kademlia distributed hash table algorithm to represent a bucket of nodes in the routing table of a node. The class provides methods to add and remove nodes from the bucket, as well as to clone the bucket and obtain a string representation of its content.

            The bucket is implemented as a **`TreeMap`** data structure, where nodes are stored as keys and the time of last contact as values. The **`addNeighbour`** method adds a new node to the bucket if it's not full, while the **`removeNeighbour`** method removes a node from the bucket given its ID. The **`clone`** method creates a new instance of the bucket with the same nodes as the original one and the **`toString`** method returns a string representation of the nodes in the bucket.

            The class also defines an empty constructor that initializes an empty **`TreeMap`** to store the nodes.

    - `BITS` and `K`: constants defined in KademliaCommonConfig that determine the size of the k-buckets (recall that`BITS` also specifies the length of the node IDs in bits, so verify!!!)
    ### **Constructors**
    - **`RoRoutingTable(int nBuckets, int k, int maxReplacements)`**: initializes the k-buckets by creating a new TreeMap and then creating and adding nBuckets number of empty KBucket objects to it.
    ### **Methods**
    - **`addNeighbour(BigInteger node)`**: takes a BigInteger node ID as an argument and adds it to the appropriate k-bucket based on its prefix length.
    - **`removeNeighbour(BigInteger node)`**: takes a BigInteger node ID as an argument and removes it from the appropriate k-bucket based on its prefix length.
    - **`getNeighbours(final int dist)`**: takes a BigInteger key and a BigInteger source node ID as arguments and returns an array of the K closest neighbours to the key. If the appropriate k-bucket for the key is full, it returns the nodes in the k-bucket. Otherwise, it retrieves the K closest nodes from all k-buckets and returns them.
    - **`clone()`**: creates a deep copy of the routing table object.
    - **`toString()`**: returns an empty string.
    - **`getBucket(BigInteger node)`**: takes a node ID, computes the log distance between the node ID of this Kademlia node and the node ID, and then returns the KBucket object at the distance index in the k_buckets TreeMap.
    - **`getBucketNum(BigInteger node)`**: takes a node ID, computes the log distance between the node ID of this Kademlia node and the node ID, and then returns the bucket number of the KBucket object at the distance index.
    -  `bucketAtDistance(int distance)`**: takes a distance index and returns the KBucket object at the corresponding distance index in the k_buckets TreeMap.
    - **`getbucketMinDistance()`**: returns the minimum distance of the routing table's buckets.
    - **`setNodeId(BigInteger id)`**: sets the nodeId of this Kademlia node to the given id.
    - **`getNodeId`**: returns the nodeId of this Kademlia node.
   
## NodeIdtoNode() Method ##
### **Description**
The **`nodeIdtoNode`** method is used to search for a node in the network with a specific **`searchNodeId`** parameter. It performs a binary search over the network nodes based on their **`nodeId`**. If the **`searchNodeId`** is found, the corresponding node is returned. If the **`searchNodeId`** is not found, a traditional search is performed for more reliability. If the **`searchNodeId`** is still not found, null is returned. 
### **Parameters**
- `searchNodeId`: the ID of the node to search for
### **Return Value**
   - `return` - the node with the given ID, or null if not found
### Exception ###
None

## getNode() Method ##
### **Description**
This **`getNode()`** method returns the **`Node`** object associated with the current instance of the Kademlia protocol. This **`Node`** object represents the node in the network and contains information such as its ID, address, and other relevant data. (Todo: verify!!!) 
- **Digression - Details of the KademliaNode class implementation**
    A **`KademliaNode`** has/consists of the following instance variables:

    - `id`: `BigInteger` value representing the node's identifier in the DHT.
    - `attackerID`: `BigInteger` value representing the identifier of the Sybil attacker node.
    - `addr`: `String` value representing the node's IP address.
    - `port`: integer value representing the port number of the node.
    - `is_evil`: boolean value indicating whether the node is a Sybil attacker or not.
    - `myTopic`: `String` value representing the topics that the node registers for.

    The class has several constructors, including one that takes an **`id`**, an **`addr`**, and a **`port`**. The other constructors allow you to specify additional information about the node such as its attacker ID and whether it is evil or not. In addition, it also implements several methods, including:
    - **`getId()`**: returns the node's ID.
    - **`getAttackerId()`**: returns the attacker ID of the node.
    - **`getAddr()`**: returns the IP address of the node.
    - **`getPort()`**: returns the port number of the node.
    - **`hashCode()`**: returns the hash code of the node's ID.
    - **`equals()`**: returns true if two nodes have the same ID.
    - **`compareTo()`**: compares two nodes based on their ID, IP address, and port number.
    - **`isEvil()`**: returns whether the node is a Sybil attacker or not.
    - **`setEvil()`**: sets whether the node is a Sybil attacker or not.
    - [ ]  Seek clarification on what specificially **`myTopic`** is used for, as it is not used in any of the methods provided in the class.
### Parameters ###
  None
### Return Value ###
   - `return` - the node associated with this Kademlia protocol instance (todo: verify!!!)

### Exception ###
  None

## handleResponse() Method ##
### **Description**
This method handles the response of a message received in response to a `ROUTE` (is this the right term to use? After all the **`Route`** method has been removed) message. The method receives a message and the sender’s PID as parameters. It starts by adding the source of the message to the routing table, if it exists. It then retrieves the corresponding FindOperation record based on the operation ID in the message. If the record exists, it updates the record with the closest set of neighbors received and adds the message ID to the operation. If there is a callback set, it calls the nodesFound method on the callback object, passing the find operation and the received neighbors.
- **Digression - callback object**
    The callback object is an instance of `KademliaEvents`, which has two abstract methods:

    - **`nodesFound(Operation op, BigInteger[] neighbours)`**: used to notify when a Kademlia operation has found a set of neighbouring nodes in the network. It takes two parameters: an instance of the `Operation` class, which represents the Kademlia operation that has found the nodes, and an array of "BigInteger" objects that represents the IDs of the neighbouring nodes.

    - **`operationComplete(Operation op)`**: used to notify when a Kademlia operation has completed its execution. It takes a single parameter: an instance of the `Operation` class, which represents the Kademlia operation that has completed.
    - [ ]  Since classes that implement the "KademliaEvents" interface must provide implementations for both methods, I am unable to find any such classes…

The method then checks if the destination node has been found. If the closest neighbors contain the destination node, it completes the operation and logs the result. If it is a `GetOperation` and the value has been found, it sets the operation to be finished, completes the operation, sets the value, and logs the result.

The method then sends as many `ROUTE` requests as possible (according to the ALPHA parameter). It does this while there are available requests and the operation has/is not finished. For each available neighbor, it creates a new request message to send to the neighbor, sets the operation ID, source, and destination of the message. It then sends the find request message to the neighbor and increments the hop count for the operation. If there are no new neighbors available and no outstanding requests, the search operation is considered finished. If it is a `PutOperation`, it creates and sends a put request to all neighbors in the neighbour's list. If it is a GetOperation, it removes the find operation record, completes the operation, and logs the result. Otherwise, it removes the find operation record and logs the result. If a callback object is set, it completes the operation and logs the result. If the body of the find operation is "Automatically Generated Traffic" and the closest neighbors contain the destination node, it updates the statistics in the KademliaObserver instance.

- [x]  What do they mean by "Automatically Generated Traffic" in the body of the message?

    There is the `TrafficGeneratorSample` class, which generates traffic containing a message whose body contains/is set to “Automatically Generated Traffic”. However, in the new/newer implementation, the `generateFindNodeMessage` replaces this with a random destination ID...

### **Parameters**
  - `m`: The received message.
  - `myPid`: The sender's PID.
### **Return Value**
  None  
### **Exceptions**
None

## handlePut() Method 
### **Description**
The `**handlePut**` method takes two parameters: the first is the message containing the put request, and the second is the PID of the sender node. It handles a `put` request received by a node, which is a request containing data ( in the message) to be stored in the key value store associated with the node. This is achieved by first extracting the data from the message and adding it to the key value store by calling the `add()` method of the kv object, which is an instance of the KeyValueStore class.

- **Digression - Details of the KeyValueStore implementation**

    The **`KeyValueStore`** class provides a simple implementation of a memory store with timeout functionality by relying on the TimeoutMemoryStore class. The KeyValueStore is basically a HashMap of key-value pairs, where the key is a BigInteger and the value is any object. It has several methods such as add, get, delete, getAll, erase, and occupancy to manipulate the store. The add method allows an object to be added to the store with an optional timeout value specified in milliseconds. If a timeout is specified, a new Timer is created and scheduled to execute a TimeoutMemoryStore after the specified timeout period.

    The `TimeoutMemoryStore` has a constructor that takes a key and a KeyValueStore instance. It implements the run method of the TimerTask interface which removes the key-value pair corresponding to the key from the KeyValueStore instance.

### **Parameters**
- `m`: message object containing the request
- `myPid`: ID of the sender node
### **Return Value**
  None
### **Exceptions**
  None

## handleFind() Method
### **Description**
This method handles the response to a `find` message in the Kademlia protocol, which is used to locate a node with a given ID or to retrieve the k closest nodes to a given ID.

The method works by first retrieving the ALPHA closest nodes to the destination node by consulting/looking into/checking the k-buckets.

- [ ]  Perhaps give more information about the k-buckets if not discussed elsewhere.

The method determines which neighbors to retrieve based on the type of message. If the message type is `MSG_FIND` or `MSG_GET`, the function retrieves the k nearest neighbors for the provided key. If the message type is `MSG_FIND_DIST`, the function retrieves the k neighbors within a distance range. Outside of these possible message types, the message type is invalid and the function returns.

The method then creates a response message containing the retrieved neighbors.
- **Digression - The response message has the following fields:**
  - Type: "MSG_RESPONSE"
  - Body: an array of BigInteger objects containing the retrieved neighbors.
  - Operation ID: set to the same operation ID as the original request message.
  - Destination: set to the original destination of the request message.
  - Source: set to the node that is handling the request. (TODO: verify)
  - ACK number: set to the ID of the original request message.

If the message type is `MSG_GET`, the method also retrieves the value associated with the provided key (if applicable) and sets it in the "value" field of the response message.

Finally, the method sends the response message back to the sender node using the **`sendMessage`** method, which takes three parameters:
  - The response message to send.
  - The ID of the node to send the message to (which is the ID of the sender node in this case).
  - The ID of the current node (which is the node that is handling the request).
### **Parameters**
- `m`: a Message object containing the request.
- `myPid`: the ID of the sender node.
### **Return Value**
None
### **Exceptions**
None

## handleInit() Method
### **Description**
The **`handleInit`** method handles the initialization of a find node operation by creating a new operation object, setting the necessary attributes, retrieving the closest nodes, and sending messages to them. Its main function is to start a find node operation, which searches for the `ALPHA` closest nodes to the provided node ID and sends a find request to them.

The method first retrieves the `ALPHA` closest nodes to the source node and adds them to the find operation, setting the available requests to `ALPHA`. It then sets the operation ID of the message and the source of the message to the itself before sending `ALPHA` messages to the closest nodes. Next, it so gets the next closest node and sends it a message. If it's a distance-based find, the hop count of the operation is incremented.

Finally, the method returns a reference to the created operation object.
### **Parameters**
  - `m`: message object containing the node ID to find
  - `myPid`: ID of the sender node
### **Return Value**
  - `return`: a reference to the created operation object
### **Exceptions**
  None

## sendMessage() Method
### **Description**
The `sendMessage` method is responsible for sending messages to other nodes in the Kademlia network using the transport layer. It also starts a timeout timer for requests.

The first step in the method is to add the destination node to the routing table ensuring that the routing table is up-to-date and contains information about all nodes in the network. Then, the source and destination nodes are obtained using their IDs (recall the `nodeIdtoNode` method is used for this purpose, which maps a node ID to a `Node` object). After that, the transport protocol is obtained from the network prototype using the transport ID (`tid`) -- currently, it seems like the transport protocol is assumed to be an instance of `UnreliableTransport`.

The `send` method of the transport protocol is then called with the source and destination nodes, the message, and the `kademliaid` (which is a unique identifier for the Kademlia network/ instance of the kademliaProtocol -- (todo: clarify!!!)).

If the message is a request (i.e., its type is `MSG_FIND` or `MSG_FIND_DIST`), a timeout timer is started. First, a `Timeout` object is created with the destination node ID, message ID, and operation ID. Then, the latency of the network between the source and destination nodes is obtained using the `getLatency()` method of the transport protocol. This latency value is used to schedule the timeout timer using the `EDSimulator.add()` method, which adds an event to the simulation event queue with a delay equal to 4 times the network latency. The message ID and timestamp are also added to the `sentMsg` map to keep track of sent messages.

- [ ]  Where is the implementation of the getLatency() method?
### **Parameters**
   - `m`: the message to send
   - `destId`: the ID of the destination node
   - `myPid`: the sender process ID
   
### **Return Value**
None
### **Exceptions**
None

## MeprocessEvent() Method
### **Description**
This `processEvent` method handles the receiving of events. It takes in the current node receiving the event, the process ID of the current node, and the event being received by the current node as input parameters. 

It first sets the Kademlia ID as the current process ID. If the event received is an instance of a Message, it reports the message to the Kademlia observer. It then handles the event based on its type. If the event is a response message, it removes it from the sentMsg map and calls `**handleResponse()**` method. If the event is an initialization message, it calls handleInit(). If the event is a find or get message, it calls `**handleFind()**`. If the event is a put message, it calls `**handlePut()**`.

~~If the event is an empty message or a store message, nothing is done, currently.~~

### **Parameters**
  - `myNode`: the current node receiving the event.
  - `myPid`: the process ID of the current node.
  - `event`: the event being received by the current node.
### **Return Value**
  None
### **Exceptions**
  None

These methods are rather self explanatory.

## getKademliaNode() Method

### **Description**
 This method returns the current `KademliaNode` object representing a node in the Kademlia network consisting of information about the node's ID, IP address, and port number.
### **Parameters**
  None
### **Return Value**
  - `KademliaNode`: 
### **Exceptions**
  None

## getRoutingTable() Method
### **Description**
This method returns the **`RoutingTable`** object associated with the **`KademliaNode`**. In other words, the **`RoutingTable`** object contains a list of **`KademliaNode`** objects, representing nodes that the **`KademliaNode`** has interacted with.
### **Parameters**
  None
### **Return Value**
  - `return`: the routing table associated with the `KademliaNode`
### **Exceptions**
  None

## setProtocolID() Method
### **Description**
  This method sets the current `kademliaid` to protocolID for the current instance of the **`KademliaProtocol`**. The protocol ID is a unique identifier for the protocol running on the node/network (todo - verify!!!).
### **Parameters**
  - `protocolID`: the protocol ID to which to set the current `kademliaid`.
### **Return Value**
  None
### **Exceptions**
  None

## getProtocolID() Method
### **Description**
  This method returns the protocol ID for the current `KademliaNode` (todo - verify!!!).
### **Parameters**
  None
### **Return Value**
 - `return`: the protocol ID for the current **`KademliaNode`**.
### **Exceptions**
  None

## setNode() Method
### **Description**
  This method sets the current **`KademliaNode`** for the Kademlia protocol. It takes a **`KademliaNode`** object as a parameter and sets it as the current node. It also sets the node ID of the current routing table to the ID of the current node.

  In addition, this it initializes and configures a logger for the current node. It sets the logger level to **`WARNING`** (important if you want to switch off the logging or set it at a lower level to avoid displaying the logs to the console) and adds a console handler to the logger to format and output log records. The log record format includes the current time, logger name, and log message.

### **Parameters**
   - `node` The KademliaNode object to set.
### **Return Value**
  None
### **Exceptions**
  None

## getLogger() Method
### **Description**
  This method returns the logger associated with the current instance of the **`KademliaProtocol`** class. The logger is an instance of the **`java.util.logging.Logger`** class and is used to log messages from the protocol.
### **Parameters**
  None
### **Return Value**
  - `return`: the logger object
### **Exceptions**
  None

## setEventsCallback() Method
### **Description**
 This method sets the callback function for events. The **`KademliaEvents`** class is an interface that defines the methods that can be called when specific events occur in the Kademlia protocol. The **`callback`** parameter is an instance of a class that implements this interface and will be called when events occur in the protocol.
### **Parameters**
   - `callback`: the callback function to set.
### **Return Value**
  None
### **Exceptions**
  None

### **4. Key Features**

- The code supports configurable parameters such as K, ALPHA, BITS, and FINDMODE, which can be set using the PeerSim configuration file.
- It supports different network/tranport protocols (e.g., UnreliableTransport) for simulating network communication.
- The code has a routing table for storing node IDs and their associated IP addresses.
- It implements various Kademlia operations, such as find, get, and put.
- It has a key-value store for storing data and a logging mechanism for tracing operations.

### **5. Example**
To simulate a network of Kademlia nodes, one can create multiple instances of KademliaProtocol and add them to the PeerSim network. The simulation can then be run using the PeerSim simulator. The code can be used to study the performance and behavior of the Kademlia protocol under various network conditions and scenarios. (Todo: Provide an actual example)

