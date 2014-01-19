-module(ensq_topic_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, send_test_srv).
%% ===================================================================
%% API functions
%% ===================================================================

start_child(Topic, Spec) ->
    supervisor:start_child(?MODULE, [Topic, Spec]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Element =
        {ensq_topic, {ensq_topic, start_link, []},
         transient, infinity, worker, [ensq_topic]},
    Children = [Element],
    RestartStrategy = {simple_one_for_one, 5, 10},
    {ok, {RestartStrategy, Children}}.

