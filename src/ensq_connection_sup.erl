-module(ensq_connection_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, send_test_srv).
%% ===================================================================
%% API functions
%% ===================================================================

start_child(Host, Port, Topic) ->
    supervisor:start_child(?MODULE, [Host, Port, Topic]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Element = {ensq_connection, {ensq_connection, start_link, []},
               transient, infinity, worker, [ensq_connection]},
    Children = [Element],
    RestartStrategy = {simple_one_for_one, 5, 10},
    {ok, {RestartStrategy, Children}}.
