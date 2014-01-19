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
-export([discover/3, add_channel/3, start_link/2, tick/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          topic,
          discovery_servers = [],
          discover_interval = 60000,
          servers = [],
          channels = []
         }).

%%%===================================================================
%%% API
%%%===================================================================

add_channel(Topic, Channel, Handler) ->
    gen_server:cast(Topic, {add_channel, Channel, Handler}).
discover(Topic, Hosts, Channels) when is_list(Hosts)->
    ensq_topic_sup:start_child(Topic, {discovery, Hosts, Channels});
discover(Topic, Host, Channels) ->
    discover(Topic, [Host], Channels).

tick() ->
    tick(self()).

tick(Pid) ->
    gen_server:cast(Pid, tick).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
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
init([Topic, {discovery, Ds, Channels}]) ->
    tick(),
    {ok, #state{topic = atom_to_list(Topic), discovery_servers = Ds,
                channels = Channels}}.

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
handle_cast({add_channel, Channel, Handler},
            State = #state{channels = Cs, servers = Ss}) ->
    ChanelB = list_to_binary(atom_to_list(Channel)),
    Topic = list_to_binary(State#state.topic),
    Ss1 = orddict:map(
            fun({Host, Port}, Pids) ->
                    E  = ensq_connection:open(Host, Port, Topic, ChanelB, Handler),
                    io:format("Reply: ~p~n", [E]),
                    {ok, Pid} = E,
                    [{Pid, Channel, Handler} | Pids]
            end, Ss),
    {noreply, State#state{servers = Ss1, channels = [{Channel, Handler} | Cs]}};

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
    D = round(I/10),
    T = I + random:uniform(D*2) - D,
    timer:apply_after(T, ensq_topic, tick, [self()]),
    {noreply, State1};

handle_cast(_Msg, State) ->
    {noreply, State}.


add_discovered(JSON, State) ->
    {ok, Producers} = jsxd:get([<<"data">>, <<"producers">>], JSON),
    Producers1 = [get_host(P) || P <- Producers],
    lists:foldl(fun add_host/2, State, Producers1).

add_host({Host, Port}, State = #state{servers = Srvs, channels = Cs}) ->
    case orddict:is_key({Host, Port}, Srvs) of
        true ->
            State;
        false ->
            Topic = list_to_binary(State#state.topic),
            io:format("New: ~s:~p", [Host, Port]),
            Pids = [{ensq_connection:open(
                       Host, Port, Topic,
                       list_to_binary(atom_to_list(Channel)), Handler),
                     Channel, Handler}|| {Channel, Handler} <- Cs],
            Pids1 = [{Pid, Channel, Handler} || {{ok, Pid}, Channel, Handler} <- Pids],
            State#state{servers = orddict:store({Host, Port}, Pids1, Srvs)}
    end.

get_host(Producer) ->
    {ok, Addr} = jsxd:get(<<"broadcast_address">>, Producer),
    {ok, Port} = jsxd:get(<<"tcp_port">>, Producer),
    {binary_to_list(Addr), Port}.

http_get(URL) ->
    case httpc:request(get, {URL,[]}, [], [{body_format, binary}]) of
        {ok,{{_,200,_}, _, Body}} ->
            {ok, jsx:decode(Body)};
        _ ->
            error
    end.

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
