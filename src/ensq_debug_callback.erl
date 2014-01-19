-module(ensq_debug_callback).

-export([response/1, message/1, error/1]).

response(Msg) ->
    io:format("[response]  ~p~n", [Msg]).

message(Msg) ->
    io:format("[Msg]  ~p~n", [Msg]).

error(Msg) ->
    io:format("[Msg]  ~p~n", [Msg]).
