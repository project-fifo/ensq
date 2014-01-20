-module(ensq).
-export([init/1, start/0, connect/3, connect/4]).


-export_type([
              topic_name/0,
              channel_name/0
             ]).

-type host() :: {Host :: inet:ip_address() | inet:hostname(),
                 Port :: inet:port_number()}.

-type single_target() :: host().

-type multi_target() :: [host()].

-type target() :: single_target() | multi_target().

-type channel_name() ::  binary().

-type channel() :: {Channel :: channel_name(), Callback :: atom()}.

-type topic_name() :: atom() | binary().

-type topic() :: {Topic :: topic_name(), [channel()], [target()]} |
                 {Topic :: topic_name(), [channel()]}.

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
