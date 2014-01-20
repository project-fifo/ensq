-module(ensq).
-export([init/1, start/0, connect/3, connect/4]).


-type host() :: {Host :: inet:ip_address() | inet:hostname(),
                 Port :: inet:port_number()}.

-type target() :: host().

-type channel() :: {Channel :: atom(), Callback :: atom()}.

-type topic() :: {Topic :: atom(), [channel()], [target()]} |
                 {Topic :: atom(), [channel()]}.

-type discovery_server() :: host().


-type spec() :: {[discovery_server()], [topic()]}.


start() ->
    application:start(inets),
    application:start(ensq).

connect(Host, Topic, Channel) ->
    connect(Host, 4150, Topic, Channel).

connect(Host, Port, Topic, Channel) ->
    ensq_connection:open(Host, Port, Topic, Channel).

-spec init(spec()) -> ok.

init({DiscoveryServers, Topics}) ->
    [topic_from_sepc(DiscoveryServers, Topic) || Topic <- Topics],
    ok.

topic_from_sepc(DiscoveryServers, {Topic, Channels}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels);
topic_from_sepc(DiscoveryServers, {Topic, Channels, []}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels);
topic_from_sepc(DiscoveryServers, {Topic, Channels, Targets}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels, Targets).
