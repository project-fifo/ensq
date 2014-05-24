-module(ensq_debug_callback).

-behaviour(ensq_channel_behaviour).

-export([response/1, message/2, error/1]).

response(Msg) ->
    io:format("[response]  ~p~n", [Msg]).

error(Msg) ->
    io:format("[error]  ~p~n", [Msg]).

message(Msg, _) ->
    io:format("[message]  ~p~n", [Msg]).

