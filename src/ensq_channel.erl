%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 18 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_channel).

-include("ensq.hrl").

%% API
-export([open/5, ready/2,
         recheck_ready_count/0,
         recheck_ready/1,
         recheck_ready_count/1,
         close/1]).

%% gen_server callbacks
-export([init/7]).

-define(SERVER, ?MODULE).

-define(RECHECK_INTERVAL, 100).

-define(FRAME_TYPE_RESPONSE, 0).
-define(FRAME_TYPE_ERROR, 1).
-define(FRAME_TYPE_MESSAGE, 2).

-record(state, {socket, buffer, current_ready_count=1,
                ready_count=1, handler=ensq_debug_callback}).

%%%===================================================================
%%% API
%%%===================================================================

open(Host, Port, Topic, Channel, Handler) ->
    Ref = make_ref(),
    Pid = spawn(ensq_channel, init, [self(), Ref, Host, Port, Topic, Channel, Handler]),
    receive
        {Ref, ok} ->
            {ok, Pid};
        {Ref, E} ->
            E
    after 1000 ->
            close(Pid),
            {error, timeout}
    end.


ready(Pid, N) ->
    Pid ! {ready, N}.

recheck_ready_count() ->
    recheck_ready_count(self()).

recheck_ready_count(Pid) ->
    erlang:send_after(?RECHECK_INTERVAL, Pid, recheck_ready).

recheck_ready(Pid) ->
    Pid ! recheck_ready.

close(Pid) ->
    Pid ! close.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(From, Ref, Host, Port, Topic, Channel, Handler) ->
    Opts = [{active, false}, binary, {deliver, term}, {packet, raw}],
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, S} ->
            lager:debug("[channel|~s:~p] connected.~n", [Host, Port]),

            lager:debug("[channel|~s:~p] Sending version.~n", [Host, Port]),
            gen_tcp:send(S, ensq_proto:encode(version)),

            lager:debug("[channel|~s:~p] Subscribing to ~s/~s.~n",
                        [Host, Port, Topic, Channel]),
            gen_tcp:send(S, ensq_proto:encode({subscribe, Topic, Channel})),

            lager:debug("[channel|~s:~p] Waiting for ack.~n", [Host, Port]),
            {ok, <<0,0,0,6,0,0,0,0,79,75>>} = gen_tcp:recv(S, 0),
            lager:debug("[channel|~s:~p] Got ack changing to active mode!~n", [Host, Port]),
            inet:setopts(S, [{active, true}]),

            lager:debug("[channel|~s:~p] Setting Ready state to 1.~n", [Host, Port]),
            gen_tcp:send(S, ensq_proto:encode({ready, 1})),

            lager:debug("[~s:~p] Done initializing.~n", [Host, Port]),
            From ! {Ref, ok},
            State = #state{socket = S, buffer = <<>>, handler = Handler},
            loop(State);
        E ->
            lager:error("[channel|~s:~p] Error: ~p~n", [Host, Port, E]),
            From ! {Ref, E}
    end.

loop(State) ->
    receive
        close ->
            terminate(State);
        {ready, 0} ->
            recheck_ready_count(),
            loop(State#state{ready_count = 0, current_ready_count=0});
        {ready, N} ->
            gen_tcp:send(State#state.socket, ensq_proto:encode({ready, N})),
            loop(State#state{ready_count = N, current_ready_count=N});
        recheck_ready ->
            State1 = case ensq_in_flow_manager:getrc() of
                         {ok, 0} ->
                             recheck_ready_count(),
                             State;
                         {ok, N} ->
                             gen_tcp:send(State#state.socket, ensq_proto:encode({ready, N})),
                             State#state{current_ready_count = N, ready_count=N}
                     end,
            loop(State1);
        {tcp, S, Data} ->
            #state{socket=S, buffer=B, ready_count=RC,
                   current_ready_count = CRC} = State,
            State1 = data(<<B/binary, Data/binary>>, CRC, State, <<>>),
            State2 = case State1#state.current_ready_count of
                         N when N < (RC / 4) ->
                             %% We don't want to ask for a propper new RC every time
                             %% this keeps the laod of the flow manager by guessing
                             %% we'll get the the same value back anyway.
                             case random:uniform(10) of
                                 10 ->
                                     ensq_in_flow_manager:getrc();
                                 _ ->
                                     ok
                             end,
                             gen_tcp:send(S, ensq_proto:encode({ready, RC})),
                             State1#state{current_ready_count = RC, ready_count=RC};
                         _ ->
                             State1
                     end,
            loop(State2);

        {tcp_closed, S} when S =:= State#state.socket ->
            terminate(State);
        Info ->
            lager:warning("Unknown message: ~p~n", [Info]),
            loop(State)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_State = #state{socket=S}) ->
    gen_tcp:close(S).

%%%===================================================================
%%% Internal functions
%%%===================================================================

data(<<Size:32/integer, Raw:Size/binary, Rest/binary>>, RC,
     State = #state{socket = S,handler = C}, Replies) ->
    Reply =
        case Raw of
            <<?FRAME_TYPE_MESSAGE:32/integer, Data/binary>> ->
                case ensq_proto:decode(Data) of
                    #message{message_id=MsgID, message=Msg} ->
                        TouchFn = fun() ->
                                          gen_tcp:send(S, ensq_proto:encode({touch, MsgID}))
                                  end,
                        case C:message(Msg, TouchFn) of
                            ok ->
                                ensq_proto:encode({finish, MsgID});
                            requeue ->
                                ensq_proto:encode({requeue, MsgID})
                        end;
                    Msg ->
                        lager:warning("[channel|~p] Unknown message ~p.",
                                      [C, Msg]),
                        <<>>
                end;
            <<?FRAME_TYPE_RESPONSE:32/integer, "_heartbeat_">> ->
                ensq_proto:encode(nop);
            <<?FRAME_TYPE_RESPONSE:32/integer, Msg/binary>> ->
                C:response(ensq_proto:decode(Msg)),
                <<>>;
            <<?FRAME_TYPE_ERROR:32/integer, Data/binary>> ->
                case ensq_proto:decode(Data) of
                    #message{message_id=MsgID, message=Msg} ->
                        case C:message(Msg) of
                            ok ->
                                ensq_proto:encode({finish, MsgID});
                            O ->
                                lager:warning("[channel|~p] ~p -> Not finishing ~s",
                                              [O, C, MsgID]),
                                <<>>
                        end
                end;
            Msg ->
                lager:warning("[channel|~p] Unknown message ~p.",
                              [C, Msg]),
                <<>>
        end,
    data(Rest, RC - 1, State, <<Replies/binary, Reply/binary>>);

data(Rest, RC, State, Reply) ->
    gen_tcp:send(State#state.socket, Reply),
    State#state{buffer=Rest, current_ready_count=RC - 1}.
