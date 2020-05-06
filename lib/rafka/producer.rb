module Rafka
  # A Kafka producer that can produce to different topics.
  # See {#produce} for more info.
  #
  # @see https://kafka.apache.org/documentation/#producerapi
  class Producer
    include GenericCommands

    # Access the underlying Redis client object
    attr_reader :redis

    # Create a new producer.
    #
    # @param [Hash] opts
    # @option opts [String] :host ("localhost") server hostname
    # @option opts [Fixnum] :port (6380) server port
    # @option opts [Hash] :redis Configuration options for the underlying
    #   Redis client
    #
    # @return [Producer]
    def initialize(opts={})
      @options = parse_opts(opts)
      @redis = Redis.new(@options)
    end

    # Produce a message to a topic. This is an asynchronous operation.
    #
    # @param topic [String]
    # @param msg [#to_s] the message
    # @param key [#to_s] an optional partition hashing key. Two or more messages
    #   with the same key will always be written to the same partition.
    #
    # @example Simple produce
    #   producer = Rafka::Producer.new
    #   producer.produce("greetings", "Hello there!")
    #
    # @example Produce two messages with a hashing key. Those messages are guaranteed to be written to the same partition
    #     producer = Rafka::Producer.new
    #     produce("greetings", "Aloha", key: "abc")
    #     produce("greetings", "Hola", key: "abc")
    def produce(topic, msg, key: nil)
      Rafka.wrap_errors do
        redis_key = "topics:#{topic}"
        redis_key << ":#{key}" if key
        @redis.rpushx(redis_key, msg.to_s)
      end
    end

    # Flush any outstanding messages. The server will block until all messages
    # are written or the provided timeout is exceeded. Note however, that the
    # provided timeout may be overshot by the `read_timeout` setting of the
    # underlying Redis client. This means that the client might interrupt the
    # call earlier than timeout_ms, if `read_timeout` is less than `timeout_ms`.
    #
    # @param timeout_ms [Fixnum]. Must be smaller than or equal to the underlying Redis client's `read_timeout`
    #   which superseeds the current timeout.
    #
    # @return [Fixnum] The number of unflushed messages
    def flush(timeout_ms=5000)
      Rafka.wrap_errors do
        redis_read_timeout = @redis._client.options.fetch(:read_timeout, 5.0)
        loops, remaining_time = timeout_ms.divmod(redis_read_timeout)
        loop_wait_time = (redis_read_timeout * 1000).to_i
        remaining_wait_time = (remaining_time * 1000).to_i

        if timeout_ms.zero?
          loop { return 0 if Integer(@redis.dump(loop_wait_time.to_s)).zero? }
        end

        unflushed_messages = 0
        loops.times do
          unflushed_messages = Integer(@redis.dump(loop_wait_time.to_s))
          break if unflushed_messages.zero?
        end

        if remaining_time.zero?
          unflushed_messages
        else
          Integer(@redis.dump(remaining_wait_time.to_s))
        end
      end
    end

    private

    # @return [Hash]
    def parse_opts(opts)
      rafka_opts = opts.reject { |k| k == :redis }
      redis_opts = opts[:redis] || {}
      REDIS_DEFAULTS.dup.merge(opts).merge(redis_opts).merge(rafka_opts)
    end
  end
end
