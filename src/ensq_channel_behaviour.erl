-module(ensq_channel_behaviour).

-callback init() ->
    {ok, State::term()}.

-callback new_frame(State::term()) ->
    {ok, State1::term()}.

-callback response(Msg::binary(), State::term()) ->
    {ok, State1::term()}.

-callback error(Msg::binary(), State::term()) ->
    {ok, State1::term()} | {error, Reason::term(), State1::term()}.

-callback message(Msg::binary(), TouchFn::fun(() -> ok | {error, term()}),
                  State::term()) ->
    {ok | requeue, State1::term()}.
