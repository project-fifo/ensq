%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 18 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_topic).

-behaviour(gen_server).

%% API
-export([get_info/1, list/0, stop/1,
         discover/3, discover/4,
         add_channel/3,
         send/2,
         start_link/2]).

%% Internal
-export([tick/1, do_retry/5]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(MAX_RETRIES, 10).

-define(RETRY_TIMEOUT, 1000).

-record(state, {
          ref2srv = [],
          topic,
          discovery_servers = [],
          discover_interval = 60000,
          servers = [],
          channels = [],
          targets = [],
          targets_rev = [],
          retry_rule,
          jitter = 0
         }).

%%%===================================================================
%%% API
%%%===================================================================

list() ->
    Children = supervisor:which_children(ensq_topic_sup),
    [get_info(Pid) || {_,Pid,_,_} <- Children].

stop(Pid) ->
    gen_server:call(Pid, stop).

get_info(Pid) ->
    gen_server:call(Pid, get_info).

add_channel(Topic, Channel, Handler) ->
    gen_server:cast(Topic, {add_channel, Channel, Handler}).

-spec discover(Topic :: ensq:topic_name(), Hosts :: [ensq:host()],
               Channels :: [ensq:channel()]) -> {ok, Pid :: pid()}.

discover(Topic, Hosts, Channels) ->
    discover(Topic, Hosts, Channels, []).

discover(Topic, Hosts, Channels, Targets) when is_list(Hosts)->
    ensq_topic_sup:start_child(Topic, {discovery, Hosts, Channels, Targets});

discover(Topic, Host, Channels, Targets) ->
    discover(Topic, [Host], Channels, Targets).


send(Topic, Msg) ->
    gen_server:call(Topic, {send, Msg}).

retry(Srv, Channel, Handler, Retry, Rule) ->
    retry(self(), Srv, Channel, Handler, Retry, Rule).

retry(Pid, Srv, Channel, Handler, Retry, {Max, Val, Type}) ->
    %% Wait retry seconds, at a maximum of 10 seconds
    %% Todo: sanitize those numbers!
    Delay = case Type of
                linear ->
                    erlang:min(Retry*Val, Max);
                quadratic ->
                    erlang:min(Retry*Retry*Val, Max)
            end,
    timer:apply_after(Delay, ensq_topic, do_retry,
                      [Pid, Srv, Channel, Handler, Retry]).

do_retry(Pid, Srv, Channel, Handler, Retry) ->
    gen_server:cast(Pid, {retry, Srv, Channel, Handler, Retry}).

tick() ->
    tick(self()).

tick(Pid) ->
    gen_server:cast(Pid, tick).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(Topic, Spec) when is_binary(Topic) ->
    gen_server:start_link(?MODULE, [Topic, Spec], []);

start_link(Topic, Spec) ->
    gen_server:start_link({local, Topic}, ?MODULE, [Topic, Spec], []).

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
init([Topic, {discovery, Ds, Channels, Targets}]) when is_binary(Topic) ->
    tick(),
    State0 = build_opts([]),
    State = State0#state{topic = binary_to_list(Topic), discovery_servers = Ds,
                         channels = Channels, targets = Targets},
    {ok, connect_targets(State)};

init([Topic, {discovery, Ds, Channels, Targets}]) ->
    tick(),
    State0 = build_opts([]),
    State = State0#state{topic = atom_to_list(Topic), discovery_servers = Ds,
                         channels = Channels, targets = Targets},
    {ok, connect_targets(State)}.

connect_targets(State = #state{targets = Targets, topic = Topic}) ->
    State#state{targets = [connect_target(Target, Topic) || Target <- Targets]}.



connect_target({Host, Port}, Topic) ->
    {ok, Pid} = ensq_connection:open(Host, Port, Topic),
    Pid;
connect_target(Targets, Topic) ->
    Pids = [ensq_connection:open(Host, Port, Topic) ||
               {Host, Port} <- Targets],
    [Pid || {ok,Pid} <- Pids].


build_opts(Opts) ->
    {ok, Interval} = application:get_env(discover_interval),
    {ok, Jitter} = application:get_env(discover_jitter),
    {ok, MaxDelay} = application:get_env(max_retry_delay),
    {ok, RetInitial} = application:get_env(retry_inital),
    {ok, RetType} = application:get_env(retry_inc_type),
    RetryRule = {MaxDelay, RetInitial, RetType},
    State = #state{discover_interval = Interval, jitter = Jitter,
                   retry_rule = RetryRule},
    build_opts(Opts, State).

%% build_opts([_ | R], State) ->
%%     build_opts(R, State);

build_opts([], State) ->
    State.



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
handle_call(stop, _From, State) ->
    {stop, normal, State};

handle_call({send, _}, _From, State = #state{targets = [], targets_rev = []}) ->
    {reply, {error, not_connected}, State};

handle_call({send, Msg}, From, State=#state{targets = [], targets_rev = Rev}) ->
    handle_call({send, Msg}, From, State#state{targets=Rev, targets_rev=[]});

handle_call({send, Msg}, From, State =
                #state{targets=[Pid | Tr], targets_rev=Rev}
           ) when is_pid(Pid) ->
    ensq_connection:send(Pid, From, Msg),
    {noreply, State#state{targets = Tr, targets_rev = [Pid | Rev]}};
handle_call({send, Msg}, From, State =
                #state{targets=[Ts | Tr], targets_rev=Rev}
           ) when is_list(Ts)->
    [ensq_connection:send(Pid, From, Msg) || {_, Pid} <- Ts],
    {noreply, State#state{targets = Tr, targets_rev = [Ts | Rev]}};

handle_call(get_info, _From, State =
                #state{
                   channels = Channels,
                   topic = Topic,
                   servers = Servers
                  }) ->
    Reply = {self(), Topic, Channels, Servers},
    {reply, Reply, State};

handle_call(Req, _From, State) ->
    lager:warning("Unknown message: ~p~n", [Req]),
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
handle_cast({retry, {Host, Port}, Channel, Handler, Retry},
            State = #state{servers = Ss, retry_rule = Rule}) ->
    Topic = list_to_binary(State#state.topic),
    case ensq_channel:open(Host, Port, Topic, Channel, Handler) of
        {ok, Pid} ->
            Ref = erlang:monitor(process, Pid),
            Entry = {Pid, Channel, Handler, Ref},
            Ss1 = orddict:append({Host, Port}, Entry, Ss),
            {noreply, State#state{servers = Ss1, ref2srv = build_ref2srv(Ss1)}};
        E ->
            lager:warning("Retry ~p of connection ~s:~p failed with ~p.~n",
                          [Retry, Host, Port, E]),
            retry({Host, Port}, Channel, Handler, Retry+1, Rule),
            {noreply, State}
    end;
handle_cast({add_channel, Channel, Handler},
            State = #state{channels = Cs, servers = Ss}) ->
    Topic = list_to_binary(State#state.topic),
    Ss1 = orddict:map(
            fun({Host, Port}, Pids) ->
                    case ensq_channel:open(Host, Port, Topic, Channel, Handler) of
                        {ok, Pid} ->
                            Ref = erlang:monitor(process, Pid),
                            [{Pid, Channel, Topic, Handler, Ref}| Pids];
                        E ->
                            lager:warning("Failed opening channel: ~p~n", [E]),
                            Pids
                    end
            end, Ss),
    {noreply, State#state{servers = Ss1, channels = [{Channel, Handler} | Cs],
                          ref2srv = build_ref2srv(Ss1)}};

handle_cast(tick, State = #state{discovery_servers = []}) ->
    {noreply, State};

handle_cast(tick, State = #state{
                             discovery_servers = Hosts,
                             topic = Topic,
                             discover_interval = I
                            }) ->
    URLTail = "/lookup?topic=" ++ Topic,
    State1 =
        lists:foldl(fun ({H, Port}, Acc) ->
                            Host = H ++ ":" ++ integer_to_list(Port),
                            URL ="http://" ++ Host ++ URLTail,
                            case http_get(URL) of
                                {ok, JSON} ->
                                    add_discovered(JSON, Acc);
                                _ ->
                                    Acc
                            end
                    end, State, Hosts),
    %% Add +/- 10% Jitter for the next discovery
    D = round(I/State#state.jitter),
    T = I + rand:uniform(D*2) - D,
    timer:apply_after(T, ensq_topic, tick, [self()]),
    {noreply, State1#state{ref2srv = build_ref2srv(State1#state.servers)}};

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

handle_info({'DOWN', Ref, _, _, _}, State =
                #state{servers=Ss, ref2srv=R2S, retry_rule = Rule}) ->
    State1 = State#state{ref2srv = lists:keydelete(Ref, 1, R2S)},
    {Ref, Srv} = lists:keyfind(Ref, 1, R2S),
    SrvData = orddict:fetch(Srv, Ss),
    case down_ref(Srv, Ref, SrvData, Rule) of
        delete ->
            {noreply, State1#state{servers=orddict:erase(Srv, Ss)}};
        SrvData1 ->
            {noreply, State1#state{servers=orddict:store(Srv, SrvData1, Ss)}}
    end;

handle_info(_, State) ->
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
terminate(_Reason,
          _ = #state{
                 targets = Ts,
                 servers = Ss
                }) ->
    [ensq_connection:close(T) || T <- Ts],
    Ss1 = [Pids || {_, Pids} <- Ss],
    Ss2 = lists:flatten(Ss1),
    [ensq_channel:close(Pid) ||{Pid, _, _, _} <- Ss2],
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

add_discovered(JSON, State) ->
    {ok, Producers} = jsxd:get([<<"producers">>], JSON),
    Producers1 = [get_host(P) || P <- Producers],
    lists:foldl(fun add_host/2, State, Producers1).

add_host({Host, Port}, State = #state{servers = Srvs, channels = Cs, ref2srv = R2S}) ->
    case orddict:is_key({Host, Port}, Srvs) of
        true ->
            State;
        false ->
            Topic = list_to_binary(State#state.topic),
            Pids = [{ensq_channel:open(Host, Port, Topic, Channel, Handler),
                     Channel, Handler} || {Channel, Handler} <- Cs],
            Pids1 = [{Pid, Channel, Handler, erlang:monitor(process, Pid)} ||
                        {{ok, Pid}, Channel, Handler} <- Pids],
            Refs = [{Ref, {Host, Port}} || {_, _, _, Ref} <- Pids1],
            State#state{servers = orddict:store({Host, Port}, Pids1, Srvs),
                        ref2srv = Refs ++ R2S}
    end.

build_ref2srv(D) ->
    build_ref2srv(D, []).
build_ref2srv([], Acc) ->
    Acc;
build_ref2srv([{_Srv, []} | R], Acc) ->
    build_ref2srv(R, Acc);
build_ref2srv([{Srv, [{_, _, _, Ref} | RR]} | R], Acc) ->
    build_ref2srv([{Srv, RR} | R], [{Ref, Srv} | Acc]).


get_host(Producer) ->
    {ok, Addr} = jsxd:get(<<"broadcast_address">>, Producer),
    {ok, Port} = jsxd:get(<<"tcp_port">>, Producer),
    {binary_to_list(Addr), Port}.

http_get(URL) ->
    case httpc:request(get, {URL,[{"Accept", "application/vnd.nsq; version=1.0"}]}, [], [{body_format, binary}]) of
        {ok,{{_,200,_}, _, Body}} ->
            {ok, jsx:decode(Body)};
        _ ->
            error
    end.

down_ref(_, Ref, [{_, _, _, Ref, _}], _) ->
    delete;
down_ref(_, _, [], _) ->
    delete;
down_ref(Srv, Ref, Records, Rule) ->
    Recods1 = lists:keydelete(Ref, 4, Records),
    case lists:keyfind(Ref, 4, Records) of
        {_, Channel, Handler, Ref} ->
            retry(Srv, Channel, Handler, 0, Rule),
            Recods1;
        _ ->
            Recods1
    end.
