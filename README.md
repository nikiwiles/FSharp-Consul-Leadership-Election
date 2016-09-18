# Demonstrating Leadership Election in Hasicorp Consul

After downloading [consul](https://www.consul.io/downloads.html) and getting it running locally, compile this project and launch four or five instances of the application at once. Each instance will attempt acquire a common leadership key / value pair, and the node that's able to obtain a lock on this key, will assume leadership. 

Each node is programmed to fail randomly, and you should be able to observe periodic leadership elections, with leadership passing from node to node, until the last node finally dies.
