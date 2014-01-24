## Erlang NSQ client
This library implements a client to the [NSQ Messaging Queue](http://bitly.github.io/nsq/) it does it's best to follow the [guidelines provided by the project](http://bitly.github.io/nsq/clients/building_client_libraries.html).

### Missing features

- Support for direct non discovery connections.
- Configuration options for discovery polling interval and jitter.
- TCP discovery (using HTTP currently).
- Proper behavior when `max_in_flight` is smaller then the total number of connections.
- Support for UTF8 hostnames (seems to be a Erlang limitation).
- Protocol negation with `IDENTIFY`.
	- max_ready_count delegation from the server.
- Reconnect intervals and progression are not configurable and apply for all even discovered connections.
- Any advanced features.
- Touch.

### Design
The Erlang process model fits the nsq client concept quite well so:
 - The top level 'process' are one (or more) topics and discovery server combinations.
 	- they can have none, one or multiple a channel + handler combinations.
 		- each discovered producer spawns one process for channel/handler that opens a own TCP connection.
 	- they can have none, one or multiple target hosts or host groups for `PUB`
	 	- Each `PUB` host spawns one sender.
	 	- Messages are send in a round robin principle.
	 	- Messages to host groups are send to all members.
	 	

```
                                 topic*
                                 /     \ 
                                /       \
                               /         \
                              /           \
                      discovery_server     \
                           /    \           \
                          /      \          target*(PUB)
                         /        \         
                        /          \       
                      server      server2
                       / \          |
                      /   \         |
                     |     |        |
                 channel* channel* channel*
                  

* - seperate process
```

### API
```erlang
%% Starts the application
ensq:start(). 

%% Setting up a topic
DiscoveryServers = [{"localhost", 4161}], %% Discovery Server
Channels = [{<<"test">>,                  %% Channel name
			  ensq_debug_callback},       %% Callback Module
			  {<<"test2">>, ensq_debug_callback}],
ensq_topic:discover(
  test,                                   %% Topic
  DiscoveryServers,                       %% Discovery servers to use
  Channels,                               %% Channels to join.
  [{"localhost", 4150}]).                 %% Targets for subing


%% Sending a message to a topic
ensq_topic:send(test, <<"hello there!">>).
```