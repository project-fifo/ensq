-record(message, {
          timestamp :: non_neg_integer(),
          message_id :: binary(),
          attempt :: non_neg_integer(),
          message :: binary()
         }).

-record(identity, {
          short_id = undefined :: undefined | binary(),
          long_id = undefined :: undefined | binary(),
          feature_negotiation = false :: boolean(),
          heartbeat_interval = -1 :: -1 | non_neg_integer(),
          output_buffer_size = 16384 :: non_neg_integer(),
          output_buffer_timeout = 250 :: non_neg_integer(),
          tls_v1 = false :: boolean(),
          snappy = false :: boolean(),
          deflate = false :: boolean(),
          deflate_level = 1 :: pos_integer(),
          sample_rate = 0 :: non_neg_integer()
         }).
