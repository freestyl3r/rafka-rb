require 'securerandom'

module Rafka
  class Consumer
    include GenericCommands

    REQUIRED = [:group, :topic]

    # The underlying Redis client object
    attr_reader :redis

    # Create a new client instance.
    #
    # @param [Hash] opts
    # @option opts [String] :host ("localhost") server hostname
    # @option opts [Fixnum] :port (6380) server port
    # @option opts [String] :topic Kafka topic to consume (required)
    # @option opts [String] :group Kafka consumer group name (required)
    # @option opts [String] :id (random) Kafka consumer id
    # @option opts [Hash] :redis ({}) Optional configuration for the
    #   underlying Redis client
    def initialize(opts={})
      @redis = Redis.new(parse_opts(opts))
      @topic = "topics:#{opts[:topic]}"
    end

    # Fetch the next message.
    #
    # @param timeout [Fixnum] the time in seconds to wait for a message
    #
    # @raise [MalformedMessageError] if the message cannot be parsed
    #
    # @return [nil, Message] the message, if any
    #
    # @example
    #   consume(5) { |msg| puts "I received #{msg.value}" }
    def consume(timeout=5)
      raised = false
      msg = nil

      Rafka.wrap_errors do
        msg = @redis.blpop(@topic, timeout: timeout)
      end

      return if !msg

      begin
        msg = Message.new(msg)
        yield(msg) if block_given?
      rescue => e
        raised = true
        raise e
      end

      msg
    ensure
      if msg && !raised
        Rafka.wrap_errors do
          @redis.rpush("acks", "#{msg.topic}:#{msg.partition}:#{msg.offset}")
        end
      end
    end

    private

    # @return [Hash]
    def parse_opts(opts)
      REQUIRED.each do |opt|
        raise "#{opt.inspect} option not provided" if opts[opt].nil?
      end

      rafka_opts = opts.reject { |k| k == :redis }
      redis_opts = opts[:redis] || {}

      options = DEFAULTS.dup.merge(rafka_opts).merge(redis_opts)
      options[:id] = SecureRandom.hex if options[:id].nil?
      options[:id] = "#{options[:group]}:#{options[:id]}"
      options
    end
  end
end
