**This is heavily in development, so this is just my brainstorming**


# Searching for peers:

You download someone else's copy of the device, and send your IP to the computer you downloaded it off. You then send that IP recursively to each of the IPs the other computer knows and the ones they know etc.

This is not secure, as each computer will know all the other computers IP addresses, Tom just suggested using a system where instead of keeping a list of IPs, you keep a list of a few IPs, and send a packet to each of them saying "Hi please forward this to all your friends! I want to download this, and I was wondering if any of you had this installed?" - I'm trying to work out hash algorithms/ work how magnet: URI schemes work to try and make this more secure - I don't know if it will though because I don't know how they work yet.

# How to find data:

### Query Flooding
> Inspired by its use in [Gnutella](https://en.wikipedia.org/wiki/Gnutella)
>
>"Gnutella once operated on a purely query flooding-based protocol. The outdated Gnutella version 0.4 network protocol employs five different packet types, namely:
>- ping: discover hosts on network  
>- pong: reply to ping  
>- query: search for a file  
>- query hit: reply to query  
>- push: download request for firewalled servants" - Wikipedia 

This sends a message to every device in the network, which makes it very easy to implement, but not scalable whatsoever. This can also open up possibilities of a DOS attack by bad actors in the network. This could be implemented using a Depth First Search, or Breadth First Search, on a graph of all devices on the network.

---
###  Distributed Hash Tables

*Note:  Most DHTs only directly support exact-match search, rather than keyword search. Apparently this can be fixed by some routing algorithm? Check Wikipedia for more details, I might mention how I could fix this is in the dissertation, but I should at least mention it as a downside.
Also I may use device and node interchangably, I am sorry.*

> ### [Pastry](https://en.wikipedia.org/wiki/Pastry_(DHT))
> This [video](https://youtu.be/WqQRQz_XYg4) is incredible for this. Pastry is an overlay network (which is a network layered on top of another network), this works by being supplied with an IP address already in the network. **Any single device can leave the network at anytime without warning with little or no chance of data loss (apparently - probably should investigate this).** This network does not use packet flooding, allowing for massive scalability! Device/Node IDs are chosen randomly (more information?), so neighbouring computers in the DHT can be nowhere near eachother on the network.
> > 'The routing overlay network is formed on top of the hash table by each peer discovering and exchanging state information consisting of a list of leaf nodes, a neighborhood list, and a routing table. The leaf node list consists of the L/2 closest peers by device/node ID in each direction around the circle.' - **Wikipedia**
> 
> I'm not sure on the necessity of the neighbourhood list, but perhaps it will become apparent.
> 
> Routing is performed by, the recipient of the packet will look in its leaf set (**???**) and send it to the closest match. I could do this by:
> -  Iterating through a list of all indexes in the routing table (each one is a hex number as given by whatever **hash algorithm** I use), and then find the look at the one before and one after, and see which has the smaller difference. This would be very inefficient for larger sets, but easy to implement.
>
> -  Looking at any which match the first digit of the packet that is not included in your hash (***Should probably specify how to work out what your hash is!!!***) and send it to the closest match. If this closest match is you, then you are either the intended recipient (if packet = your hash) or it doesn't exist (if packet != your hash), This is assuming the hash table contains all combinations for all of the digits in your hash. This is most likely preferable, as you are not iterating through your entire list, which may be sizable for larger hash sizes.

>### [Chord](https://en.wikipedia.org/wiki/Chord_(peer-to-peer))
> Since this is incredibly similar to Pastry (as shown above), I will state the main differences.
> - Chord is fully 'ring based'. This means each device holds information about parts of the ring and routes packets to the next device in the ring. I think this may have disadvantages, becuase if the entire subset is offline, it won't be able to send the packets to anyone.
> - In chord, each device holds a 'successor list', this means that it can route *around* failed devices in the ring. This successor list holds the next *k* nodes (typically 3-4) in the ring after the device.
> - This also does not support keyword search, similarly to Pastry.
>
>   ![image](image_2.png)
>
>   *A 16-node Chord network. The "fingers" for one of the nodes are in black.*
> - **Finger Tables**: Chord makes each device keep a finger table containing up to *m* entries. where *m* is the number of bits in the hash key. The *i*th entry of node *n* will contain ![image](image.png)This seems relatively similar to a systematic sample to me, but you start from *n* instead of a randomly generated number. The first entry of the finger table is the device's immediate successor, and every time a device wants to look up a key *k*, it will send the query to the closest successor or predecessor (depending on the finger table) in the finger table, until a node finds out the key is stored in its immediate successor.
>
>
>   *I don't understand this, but it could be to do with how each each device's 'successor list'. If i pursue this method I will have to do some maths on how to calculate the size of the succesor list.*

>### [Kademlia](https://en.wikipedia.org/wiki/Kademlia)
>[ORIGINAL PAPER - Kademlia: A Peer-to-peer Information System
Based on the XOR Metric](http://www.scs.stanford.edu/~dm/home/papers/kpos.pdf)
>
>*The original paper is actually really good at explaining it, but I think I give an introduction here.*
>
> Devices communicate amongst eachother using UDP (User Datagram Protocol), UDP has no handshakes, so you would need TCP (Transmission Control Protocol) or equivelant to check for packet loss etc. These communications specify the structure of the network and are also used to exchange data. Each device is identified by a UUID, this is used for identification and by the **Kademlia Algorithm** to locate values, such as keywords or file hashes. Similarly to Pastry, it finds devices closer and closer to the key until the contacted device returns the value or no closer nodes are found. Kademlia calculates distances between nodes by using XOR upon the key and device UUID. This is because:
> - the distance between a node and itself is zero
> - it is symmetric: the "distances" calculated from A to B and from B to A are the same
> - it follows the triangle inequality: given A, B and C are corners of a triangle, then the distance from A to B is shorter than or equal to the sum of both the distance from A to C and the distance from C to B.
> - It is incredibly cheap and simple.
>
> Also, a network with 2^n nodes will only take n steps (worst case). So it is incredibly efficient.
>
> ![image](image_3.png)
>
> *Visual depiction of Kademlia's routing*

***NOT FINISHED WITH KADEMLIA YET, AND THERE MAY BE SOME CHANGES TO BE MADE TO CHORD***
