-module(ensq_channel_behaviour).

-callback response(Msg::binary()) ->
    ok.
-callback error(Msg::binary()) ->
    ok | {error, Reason::term()}.

-callback message(Msg::binary(), TouchFn::fun(() -> ok | {error, term()})) ->
    ok | requeue.
