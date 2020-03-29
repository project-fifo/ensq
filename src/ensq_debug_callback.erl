-module(ensq_debug_callback).

-behaviour(ensq_channel_behaviour).

-export([init/0, response/2, message/3, error/2]).

init() ->
    {ok, undefined}.

response(Msg, State) ->
    logger:debug("[response]  ~p~n", [Msg]),
    {ok, State}.

error(Msg, State) ->
    logger:debug("[error]  ~p~n", [Msg]),
    {ok, State}.

message(Msg, _, State) ->
    logger:debug("[message]  ~p~n", [Msg]),
    {ok, State}.

