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

%% API
-export([open/3,
         start_link/3,
         send/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(RECHECK_INTERVAL, 100).

-record(state, {socket, buffer, topic, from}).

%%%===================================================================
%%% API
%%%===================================================================

open(Host, Port, Topic) ->
    ensq_connection_sup:start_child(Host, Port, Topic).

send(Pid, From, Topic) ->
    gen_server:cast(Pid, {send, From, Topic}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
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
    Opts = [{active, true}, binary, {deliver, term}, {packet, raw}],
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, Socket} ->
            io:format("[~s:~p/~p] target connected.~n", [Host, Port, Topic]),
            gen_tcp:send(Socket, ensq_proto:encode(version)),
            {ok, #state{socket = Socket, buffer = <<>>, topic = list_to_binary(Topic)}};
        E ->
            io:format("[~s:~p]  target Error: ~p~n", [Host, Port, E]),
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

handle_cast({send, From, Msg}, State=#state{socket=S, topic=Topic}) ->
    gen_tcp:send(S, ensq_proto:encode({publish, Topic, Msg})),
    {noreply, State#state{from = From}};
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

handle_info({tcp, S, Data}, State=#state{socket=S, buffer=B}) ->
    State1 = data(State#state{buffer = <<B/binary, Data/binary>>}),
    {noreply, State1};

handle_info({tcp_closed, S}, State = #state{socket = S}) ->
    io:format("Stopping.~n"),
    {stop, normal, State};

handle_info(Info, State) ->
    io:format("Unknown message: ~p~n", [Info]),
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

data(State = #state{buffer = <<Size:32/integer, Raw:Size/binary, Rest/binary>>,
                    socket = S, from = From}) ->
    case Raw of
        <<0:32/integer, "_heartbeat_">> ->
            gen_tcp:send(S, ensq_proto:encode(nop));
        <<0:32/integer, Data/binary>> ->
            case ensq_proto:decode(Data) of
                {message, _Timestamp, MsgID, Msg} ->
                    io:format("[rsp:~s] ~p", [MsgID, Msg]);
                Msg ->
                    gen_server:reply(From, Msg)
            end;
        <<1:32/integer, Data/binary>> ->
            case ensq_proto:decode(Data) of
                {message, _Timestamp, MsgID, Msg} ->
                    io:format("[msg:~s] ~p", [MsgID, Msg]);
                Msg ->
                    io:format("[msg] ~p", [Msg])
            end;
        <<2:32/integer, Data/binary>> ->
            case ensq_proto:decode(Data) of
                {message, _Timestamp, MsgID, Msg} ->
                    io:format("[err:~s] ~p", [MsgID, Msg]);
                Msg ->
                    io:format("[err] ~p", [Msg])
            end;
        Msg ->
            io:format("[unknown] ~p~n", [Msg])
    end,
    data(State#state{buffer=Rest});

data(State) ->
    State.
