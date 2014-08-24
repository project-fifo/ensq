-module(ensq_proto).

-compile(inline).

-export([encode/1, decode/1]).

-include("ensq.hrl").

-define(IDENTITY_FIELD(F, J), [{F, I#identity.F} | J]).

-define(IDENTITY_FIELD_OPT(F, J), case I#identity.F of
                                      undefined ->
                                          J;
                                      _ ->
                                          [{F, I#identity.F} | J]
                                  end).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

decode(<<"OK">>) -> ok;
decode(<<"E_INVALID">>) -> {error, invalid};
decode(<<"E_BAD_BODY">>) -> {error, bad_body};
decode(<<"E_BAD_TOPIC">>) -> {error, bad_topic};
decode(<<"E_BAD_CHANNEL">>) -> {error, bad_channel};
decode(<<"E_BAD_MESSAGE">>) -> {error, bad_message};
decode(<<"E_PUB_FAILED">>) -> {error, pub_failed};
decode(<<"E_MPUB_FAILED">>) -> {error, mpub_failed};
decode(<<"E_FIN_FAILED">>) -> {error, fin_failed};
decode(<<"E_REQ_FAILED">>) -> {error, req_failed};
decode(<<"CLOSE_WAIT">>) -> close_wait;
decode(<<Timestamp:64/integer, Attempt:16/integer,
         MsgID:16/binary, Msg/binary>>) ->
    #message{
       timestamp = Timestamp,
       message_id = MsgID,
       attempt = Attempt,
       message = Msg
      };
decode(M) -> {error, unknown, M}.

encode(version) ->
    <<"  V2">>;
encode(nop) ->
    <<"NOP\n">>;
encode(close) ->
    <<"CLS\n">>;
encode({identify, #identity{} = I}) ->
    JSON =  identity_to_json(I),
    <<"IDENTIFY\n", (byte_size(JSON)):32, JSON/binary>>;
encode({subscribe, Topic, Channel})
  when is_binary(Channel), is_binary(Topic) ->
    <<"SUB ", Topic/binary, $\s, Channel/binary, $\n>>;
encode({publish, Topic, Data})
  when is_binary(Topic),
       is_binary(Data) ->
    <<"PUB ", Topic/binary, $\n, (byte_size(Data)):32, Data/binary>>;
encode({publish, Topic, [_E | _] = Messages})
  when is_binary(Topic),
       is_binary(_E) ->
    Data = << <<(byte_size(D)):32, D/binary>> || D <- Messages>>,
    Count = length(Messages),
    Body = <<Count:32, Data/binary>>,
    <<"MPUB ", Topic/binary, $\n, (byte_size(Body)):32, Body/binary>>;
encode({ready, N})
  when is_integer(N), N >= 0 ->
    <<"RDY ", (i2b(N))/binary, $\n>>;
encode({finish, MsgID})
  when is_binary(MsgID) ->
    <<"FIN ", MsgID/binary, $\n>>;
encode({requeue, MsgID, Timeout})
  when is_binary(MsgID),
       is_integer(Timeout),
       Timeout >= 0 ->
    <<"REQ ", MsgID/binary, $\s, (i2b(Timeout))/binary, $\n>>;
encode({touch, MsgID})
  when is_binary(MsgID) ->
    <<"TOUCH ", MsgID/binary, $\n>>.

i2b(I) ->
    integer_to_binary(I).
identity_to_json(#identity{} = I) ->
    J = ?IDENTITY_FIELD_OPT(short_id, []),
    J0 = ?IDENTITY_FIELD_OPT(long_id, J),
    J1 = ?IDENTITY_FIELD(feature_negotiation, J0),
    J2 = ?IDENTITY_FIELD(heartbeat_interval, J1),
    J3 = ?IDENTITY_FIELD(output_buffer_size, J2),
    J4 = ?IDENTITY_FIELD(output_buffer_timeout, J3),
    J5 = ?IDENTITY_FIELD(tls_v1, J4),
    J6 = ?IDENTITY_FIELD(snappy, J5),
    J7 = ?IDENTITY_FIELD(deflate, J6),
    J8 = ?IDENTITY_FIELD(deflate_level, J7),
    J9 = ?IDENTITY_FIELD(sample_rate, J8),
    jsx:encode(J9).

-ifdef(TEST).

%% (nsq.identify,
%%  {'data': identify_dict_ascii},
%%  'IDENTIFY\n' + struct.pack('>l', len(identify_body_ascii)) +
%%      identify_body_ascii),
%%                 (nsq.identify,
%%                     {'data': identify_dict_unicode},
%%                     'IDENTIFY\n' + struct.pack('>l', len(identify_body_unicode)) +
%%                     identify_body_unicode),
identify_test() ->
    JSON = identity_to_json(#identity{}),
    Bin = <<"IDENTIFY\n", (byte_size(JSON)):32, JSON/binary>>,
    ?assertEqual(Bin, encode({identify, #identity{}})).

subscribe_test() ->
    Bin = <<"SUB test_topic test_channel\n">>,
    ?assertEqual(Bin, encode({subscribe, <<"test_topic">>, <<"test_channel">>})).

fin_test() ->
    Bin = <<"FIN test\n">>,
    ?assertEqual(Bin, encode({finish, <<"test">>})).

req_test() ->
    Bin = <<"REQ test 0\n">>,
    ?assertEqual(Bin, encode({requeue, <<"test">>, 0})),
    Bin1 = <<"REQ test 60\n">>,
    ?assertEqual(Bin1, encode({requeue, <<"test">>, 60})).

touch_test() ->
    Bin = <<"TOUCH test\n">>,
    ?assertEqual(Bin, encode({touch, <<"test">>})).

rdy_test() ->
    Bin = <<"RDY 100\n">>,
    ?assertEqual(Bin, encode({ready, 100})).

nop_test() ->
    Bin = <<"NOP\n">>,
    ?assertEqual(Bin, encode(nop)).

cls_test() ->
    Bin = <<"CLS\n">>,
    ?assertEqual(Bin, encode(close)).

pub_test() ->
    Bin = <<"PUB test\n", 3:32, "ABC">>,
    ?assertEqual(Bin, encode({publish, <<"test">>, <<"ABC">>})).

mpub_test() ->
    Bin = <<"MPUB test\n", 17:32, 2:32, 3:32, "ABC", 2:32, "DE">>,
    ?assertEqual(Bin, encode({publish, <<"test">>, [<<"ABC">>, <<"DE">>]})).

-endif.
