-module(ensq_channel_behaviour).

-callback init() ->
    {ok, State::term()}.

-callback response(Msg::binary(), State::term()) ->
    {ok, State::term()}.

-callback error(Msg::binary(), State::term()) ->
    {ok, State::term()} | {error, Reason::term()}.

-callback message(Msg::binary(), TouchFn::{term(), binary()}, State::term()) ->
    {ok, State::term()} | {requeue, State::term(), Timeout::non_neg_integer()}.
