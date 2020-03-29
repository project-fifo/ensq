## Erlang NSQ client
This library implements a client to the [NSQ Messaging Queue](http://bitly.github.io/nsq/) it does it's best to follow the [guidelines provided by the project](http://bitly.github.io/nsq/clients/building_client_libraries.html).

### Missing features

- Support for direct non discovery connections.
- TCP discovery (using HTTP currently).
- Proper behavior when `max_in_flight` is smaller then the total number of connections.
- Support for UTF8 hostnames (seems to be a Erlang limitation).
- Protocol negation with `IDENTIFY`.
	- max_ready_count delegation from the server.

### OTP Compatibility

OTP 21.0 or above is required since 0.3.0,
If you need a version of ensq which runs on an older OTP release,
we suggest you use ensq 0.2.0.

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

#### Using `ensq:init`
One way to connect to nsq is using

#### Setting up topics separately
```
%% Setting up a topic separately.
DiscoveryServers = [{"localhost", 4161}], %% Discovery Server
Channels = [{<<"test">>,                  %% Channel name
			  ensq_debug_callback},       %% Callback Module
			  {<<"test2">>, ensq_debug_callback}],
ensq_topic:discover(
  test,                                   %% Topic
  DiscoveryServers,                       %% Discovery servers to use
  Channels,                               %% Channels to join.
  [{"localhost", 4150}]).                 %% Targets for SUBing


%% Sending a message to a topic
ensq:send(test, <<"hello there!">>).
```

### Non registered channels
By default channels are registered processes that way it's possible to do things like `ensq:send(test, ...)` but for some situations registering the process and creating an `atom` for the name is not desirable to prevent poisoning the atom table. A dynamically created and destroyed channel would be an example of this. Passing a `binary` instead of a `atom` binary to `ensq_topic:discover/4` will solve this issue but loose the advantage of being able to call the process by name, it still is possible to either save the `PID` on creation or retrieve it from `ensq:list/1`.

### Configuration

- `max_in_flight` maximum number of messages the server can handle at once (`300`).
- `discovery_inteval` Delay between discovery requests in milliseconds (`60000`).
- `discover_jitter` Jitter in percent added or subtracted of the discovery interval, a value of `10` gives a total jitter of `20%` by adding between `-10%` and `+10%` (`10`).
- `max_retry_delay` The maximum delay for retries in milliseconds (`10000`).
- `retry_inital` Initial retry value in milliseconds (`1000`).
- `retry_inc_type` The algorithm the retry interval progresses, it can either be `linear` making the retry delay `Attempt*retry_inital` milliseconds or `quadratic` making the retry delay `Attempt*Attempt*retry_inital`
