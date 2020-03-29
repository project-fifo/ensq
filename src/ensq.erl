-module(ensq).
-export([start/0,
         init/1,
         producer/2, producer/3,
         list/0,
         send/2,
         touch/1]).


-export_type([
              host/0,
              channel/0,
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
    application:start(syntax_tools),
    application:start(compiler),
    application:start(ensq).

%%--------------------------------------------------------------------
%% @doc
%% This function is used to initialize one or more topics on a given
%% set of discovery servers. This call can be done multiple times
%% in the case different discovery servers are used for different
%% topic sets.
%%
%% @end
%%--------------------------------------------------------------------

-spec init(spec()) -> ok.

init({DiscoveryServers, Topics}) ->
    [topic_from_sepc(DiscoveryServers, Topic) || Topic <- Topics],
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all currently known discoveryserver/topic
%% combinations.
%%
%% @end
%%--------------------------------------------------------------------
list() ->
    ensq_topic:list().

%%--------------------------------------------------------------------
%% @doc
%% Sends a message to a topics target servers.
%%
%% @end
%%--------------------------------------------------------------------
send(Topic, Msg) when is_binary(Msg),
                      is_pid(Topic) orelse is_atom(Topic) ->
    ensq_topic:send(Topic, Msg).

%%--------------------------------------------------------------------
%% @doc
%% Creates a producer connection to a single host.
%%
%% @end
%%--------------------------------------------------------------------
-spec producer(Channel::atom()|binary(),
               Host::inet:ip_address() | inet:hostname(),
               Port::inet:port_number()) ->
                      {ok, Pid::pid()} | {error, Reason::term()}.
producer(Channel, Host, Port) ->
    producer(Channel, [{Host, Port}]).

%%--------------------------------------------------------------------
%% @doc
%% Creates a producer connection to multiple hosts.
%%
%% @end
%%--------------------------------------------------------------------
-spec producer(Channel::atom()|binary(),
               Targets :: [host()]) ->
                      {ok, Pid::pid()} | {error, Reason::term()}.
producer(Channel, Targets) ->
    ensq_topic:discover(Channel, [], [], Targets).

topic_from_sepc(DiscoveryServers, {Topic, Channels}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels);
topic_from_sepc(DiscoveryServers, {Topic, Channels, []}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels);
topic_from_sepc(DiscoveryServers, {Topic, Channels, Targets}) ->
    ensq_topic:discover(Topic, DiscoveryServers, Channels, Targets).

touch({S, MsgID}) ->
    gen_tcp:send(S, ensq_proto:encode({touch, MsgID})).
