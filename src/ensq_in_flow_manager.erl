%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 19 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_in_flow_manager).

-behaviour(gen_server).

%% API
-export([start_link/0, getrc/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {max_in_flight, channels=[], last_count=0}).

%%%===================================================================
%%% API
%%%===================================================================

getrc() ->
    gen_server:call(?SERVER, getrc).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
    {ok, Max} = application:get_env(max_in_flight),
    {ok, #state{max_in_flight=Max}}.

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
handle_call(getrc, {From, _}, State=#state{channels=Cs, max_in_flight=Max}) ->
    case lists:keyfind(From, 2, Cs) of
        false ->
            InUse = lists:sum([N || {N, _, _} <- Cs]),
            Free = Max - InUse,
            {N, CsN} =
                case length(Cs) of
                    %% We have more channels then we have max in flight
                    %% so this one will have to wait.
                    L when L >= Max ->
                        {0, Cs};
                    %% This is the first channel we hand out 50%
                    0 ->
                        {erlang:trunc(Max*0.5), Cs};
                    %% If we have more then the average free we can jsut go and
                    %% use that
                    L when Free > Max/L ->
                        Avg = erlang:trunc(Max*0.5/L),
                        {Avg, Cs};
                    %% We have only a few channels so we we might be able to
                    %% grab a few from the largest channel.
                    L when L < (Max / 20) ->
                        [{NMax, PidMax, RefMax} | CsS] = lists:sort(Cs),
                        N1 = erlang:trunc(NMax/2),
                        ensq_channel:ready(PidMax, N1),
                        {N1, [{NMax, PidMax, RefMax} | CsS]};
                    %% We'll have to reshuffle the while ready count sorry.
                    %% Every channel will get a equal share of 75% of
                    L ->
                        RC = erlang:trunc((Max/L)*0.75),
                        Cs1 = [{RC, P, R} || {_, P, R} <- Cs],
                        [ensq_channel:ready(Pid, N) || {N, Pid, _} <- Cs1],
                        {RC, Cs1}
                end,
            Ref = erlang:monitor(process, From),
            {reply, {ok, N}, State#state{channels=[{N, From, Ref}|CsN]}};
        %% We got a receive count!
        {N, From, Ref} ->
            %% If we only have one channel we slowsly scale this up
            case length(Cs) of
                1 ->
                    RC1 = erlang:min(Max*0.9, N*1.1),
                    {reply, {ok, erlang:trunc(RC1)}, State};
                L ->
                    Avg = Max / L,
                    InUse = lists:sum([Nu || {Nu, _, _} <- Cs]),
                    Free = Max - InUse,
                    NewN = erlang:trunc(erlang:min(Free, Avg)),
                    CsN = lists:keydelete(From, 2, Cs),
                    {reply, {ok, NewN},
                     State#state{channels=[{NewN, From, Ref}|CsN]}}
            end
    end;

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
handle_info({'DOWN', Ref, _, _, _}, State = #state{channels=Cs}) ->
    {noreply, State#state{channels=lists:keydelete(Ref, 3, Cs)}};
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
