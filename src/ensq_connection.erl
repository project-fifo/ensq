%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 18 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_connection).

-behaviour(gen_server).

-include("ensq.hrl").

%% API
-export([open/3, close/1,
         start_link/3,
         send/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(RECHECK_INTERVAL, 100).

-record(state, {socket, buffer, topic, from = queue:new(), host, port}).

%%%===================================================================
%%% API
%%%===================================================================

open(Host, Port, Topic) ->
    ensq_connection_sup:start_child(Host, Port, Topic).

send(Pid, From, Topic) ->
    gen_server:cast(Pid, {send, From, Topic}).

close(Pid) ->
    gen_server:call(Pid, close).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(Host, Port, Topic) ->
    gen_server:start_link(?MODULE, [Host, Port, Topic], []).

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
init([Host, Port, Topic]) ->
    {ok, connect(#state{topic = list_to_binary(Topic),
                        host = Host, port = Port})}.

connect(State = #state{host = Host, port = Port}) ->
    lager:info("[~s:~p] Connecting.", [Host, Port]),

    case State#state.socket of
        undefined ->
            ok;
        Old ->
            lager:info("[~s:~p] Closing as part of connect.", [Host, Port]),
            gen_tcp:close(Old)
    end,
    Opts = [{active, true}, binary, {deliver, term}, {packet, raw}],
    State1 = State#state{socket = undefined, buffer = <<>>, from = queue:new()},
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, Socket} ->
            case gen_tcp:send(Socket, ensq_proto:encode(version)) of
                ok ->
                    lager:info("[~s:~p] Connected to: ~p.",
                               [Host, Port, Socket]),
                    State1#state{socket = Socket};
                E ->
                    lager:info("[~s:~p] Connection errror: ~p.",
                               [Host, Port, E]),
                    State1
            end;
        E ->
            lager:error("[~s:~p] target Error: ~p~n", [Host, Port, E]),
            State1
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
handle_call(close, _From, State) ->
    {stop, normal, ok, State};

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

handle_cast({send, From, Msg}, State=#state{socket=S, topic=Topic, from = F}) ->
    State1 = case S of
                 undefined ->
                     connect(State);
                 _ ->
                     State
             end,
    case State1#state.socket of
        undefined ->
            gen_server:reply(From, {error, not_connected}),
            {noreply, State1};
        S1 ->
            case gen_tcp:send(S1, ensq_proto:encode({publish, Topic, Msg})) of
                ok ->
                    gen_server:reply(From, ok),
                    {noreply, State1#state{from = queue:in(From, F)}};
                E ->
                    lager:warning("[~s] Ooops: ~p~n", [Topic, E]),
                    gen_server:reply(From, E),
                    {noreply, connect(State1)}
            end
    end;

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

handle_info({tcp, S, Data}, State=#state{ socket = S, buffer=B}) ->
    State1 = data(State#state{buffer = <<B/binary, Data/binary>>}),
    {noreply, State1};

handle_info({tcp, S, Data}, State=#state{socket = S0, buffer=B}) ->
    lager:info("[~s:~p] Got data from ~p but socket should be ~p.",
               [State#state.host, State#state.port, S, S0]),
    State1 = data(State#state{buffer = <<B/binary, Data/binary>>, socket = S}),
    {noreply, State1#state{socket = S0}};

handle_info({tcp_closed, S}, State = #state{socket = S}) ->
    lager:info("[~s:~p] Remote side hung up.",
               [State#state.host, State#state.port]),
    {noreply, connect(State)};

handle_info(_Info, State) ->
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
terminate(_Reason, _State = #state{socket = S}) ->
    gen_tcp:close(S),
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

data(State = #state{buffer = <<Size:32/integer, Raw:Size/binary, Rest/binary>>,
                    socket = S, from = F}) ->
    R =
        case Raw of
            <<0:32/integer, "_heartbeat_">> ->
                gen_tcp:send(S, ensq_proto:encode(nop));
            <<0:32/integer, Data/binary>> ->
                {{value,From}, F1} = queue:out(F),
                case ensq_proto:decode(Data) of
                    #message{message_id=MsgID, message=Msg} ->
                        gen_tcp:send(S, ensq_proto:encode({finish, MsgID})),
                        gen_server:reply(From, Msg),
                        {state, State#state{from = F1}};
                    Msg ->
                        gen_server:reply(From, Msg),
                        {state, State#state{from = F1}}
                end;
            <<1:32/integer, Data/binary>> ->
                case ensq_proto:decode(Data) of
                    #message{message_id=MsgID, message=Msg} ->
                        gen_tcp:send(S, ensq_proto:encode({finish, MsgID})),
                        lager:info("[msg:~s] ~p", [MsgID, Msg]);
                    Msg ->
                        lager:info("[msg] ~p", [Msg])
                end;
            <<2:32/integer, Data/binary>> ->
                {{value,From}, F1} = queue:out(F),
                case ensq_proto:decode(Data) of
                    #message{message_id=MsgID, message=Msg} ->
                        gen_tcp:send(S, ensq_proto:encode({finish, MsgID})),
                        gen_server:reply(From, Msg),
                        {state, State#state{from = F1}};
                    Msg ->
                        gen_server:reply(From, Msg),
                        {state, State#state{from = F1}}
                end;
            Msg ->
                lager:warning("[unknown] ~p~n", [Msg])
        end,
    State1 = case R of
                 {state, StateX} ->
                     StateX#state{buffer=Rest};
                 _ ->
                     State#state{buffer=Rest}
             end,
    data(State1);

data(State) ->
    State.
