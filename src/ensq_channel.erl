%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 18 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_channel).

-behaviour(gen_server).

-include("ensq.hrl").

%% API
-export([open/5, ready/2,
         start_link/5, recheck_ready_count/0,
         recheck_ready/1,
         recheck_ready_count/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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
    ensq_channel_sup:start_child(Host, Port, Topic, Channel, Handler).

ready(Pid, N) ->
    gen_server:cast(Pid, {ready, N}).

recheck_ready_count() ->
    recheck_ready_count(self()).

recheck_ready_count(Pid) ->
    timer:apply_after(?RECHECK_INTERVAL, ensq_channel, recheck_ready, [Pid]).

recheck_ready(Pid) ->
    gen_server:cast(Pid, recheck_ready).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(Host, Port, Topic, Channel, Handler) ->
    gen_server:start_link(?MODULE, [Host, Port, Topic, Channel, Handler], []).

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
init([Host, Port, Topic, Channel, Handler]) ->
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
            {ok, #state{socket = S, buffer = <<>>, handler = Handler}};
        E ->
            lager:error("[channel|~s:~p] Error: ~p~n", [Host, Port, E]),
            {stop, E}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({ready, 0}, State) ->
    recheck_ready_count(),
    {noreply, State#state{ready_count = 0, current_ready_count=0}};

handle_cast({ready, N}, State = #state{socket = S}) ->
    gen_tcp:send(S, ensq_proto:encode({ready, N})),
    {noreply, State#state{ready_count = N, current_ready_count=N}};

handle_cast(recheck_ready, State = #state{socket = S, ready_count=0}) ->
    State1 = case ensq_in_flow_manager:getrc() of
                 {ok, 0} ->
                     recheck_ready_count(),
                     State;
                 {ok, N} ->
                     gen_tcp:send(S, ensq_proto:encode({ready, N})),
                     State#state{current_ready_count = N, ready_count=N}
             end,
    {noreply, State1};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_info({tcp, S, Data}, State=#state{socket=S, buffer=B, ready_count=RC}) ->
    State1 = data(<<B/binary, Data/binary>>, State),
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
    {noreply, State2};

handle_info({tcp_closed, S}, State = #state{socket = S}) ->
    {stop, normal, State};

handle_info(Info, State) ->
    lager:warning("Unknown message: ~p~n", [Info]),
    {noreply, State}.

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
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

data(<<Size:32/integer, ?FRAME_TYPE_MESSAGE:32/integer,
       Data:(Size-4)/binary, Rest/binary>>,
     RC, State = #state{handler = C}) ->
    #message{message_id=MsgID, message=Msg} = ensq_proto:decode(Data),
    TouchFn = fun() ->
                      gen_tcp:send(S, ensq_proto:encode({touch, MsgID}))
              end,
    case C:message(Msg, TouchFn) of
        ok ->
            gen_tcp:send(S, ensq_proto:encode({finish, MsgID}));
        requeue ->
            gen_tcp:send(S, ensq_proto:encode({requeue, MsgID}))
    end,
    data(Rest, RC - 1, State);

data(<<15:32/integer, ?FRAME_TYPE_RESPONSE:32/integer,
       "_heartbeat_", Rest/binary>>,
     RC, State = #state{socket = S}) ->
    gen_tcp:send(S, ensq_proto:encode(nop)),
    data(Rest, RC -1, State);

data(<<Size:32/integer, ?FRAME_TYPE_RESPONSE:32/integer,
       Msg:(Size-4)/binary, Rest/binary>>,
     RC, State = #state{handler = C}) ->
    C:response(ensq_proto:decode(Msg)),
    data(Rest, RC - 1, State);

data(<<Size:32/integer, ?FRAME_TYPE_ERROR:32/integer,
       Data:(Size-4)/binary, Rest/binary>>,
     RC, State = #state{handler = C}) ->
    #message{message_id=MsgID, message=Msg} = ensq_proto:decode(Data),
    case C:message(Msg) of
        ok ->
            gen_tcp:send(S, ensq_proto:encode({finish, MsgID}));
        O ->
            lager:warning("[channel|~p] ~p -> Not finishing ~s", [O, C, MsgID])
    end,
    data(Rest, RC - 1, State);

data(<<Size:32/integer, Raw:Size/binary, Rest/binary>>,
     RC, State = #state{handler = C}) ->
    lager:warning("[channel|~p] Unknown message ~p.", [C, Raw]),
    data(Rest, RC-1, State);

data(Rest, RC, State) ->
    State#state{buffer=Rest, current_ready_count=RC}).
