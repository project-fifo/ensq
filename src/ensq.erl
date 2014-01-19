-module(ensq).
-export([start/0, connect/3, connect/4]).

start() ->
    application:start(inets),
    application:start(ensq).

connect(Host, Topic, Channel) ->
    connect(Host, 4150, Topic, Channel).

connect(Host, Port, Topic, Channel) ->
    ensq_connection:open(Host, Port, Topic, Channel).
