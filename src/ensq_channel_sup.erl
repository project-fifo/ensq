-module(ensq_channel_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/5]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, send_test_srv).
%% ===================================================================
%% API functions
%% ===================================================================

start_child(Host, Port, Topic, Channel, Handler) ->
    supervisor:start_child(?MODULE, [Host, Port, Topic, Channel, Handler]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Element = {ensq_channel, {ensq_channel, start_link, []},
               temporary, infinity, worker, [ensq_channel]},
    Children = [Element],
    RestartStrategy = {simple_one_for_one, 5, 10},
    {ok, {RestartStrategy, Children}}.
