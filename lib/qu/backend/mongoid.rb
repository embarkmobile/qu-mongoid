require 'mongoid'

module Qu
  module Backend
    class Mongoid < Base

      # Number of times to retry connection on connection failure (default: 5)
      attr_accessor :max_retries

      # Seconds to wait before try to reconnect after connection failure (default: 1)
      attr_accessor :retry_frequency

      # Seconds to wait before looking for more jobs when the queue is empty (default: 5)
      attr_accessor :poll_frequency
      
      attr_accessor :client_name

      def initialize
        self.max_retries     = 5
        self.retry_frequency = 1
        self.poll_frequency  = 5
        self.client_name = :default
      end

      def connection
        Thread.current[self.to_s] ||= begin
          unless ::Mongoid.clients[@client_name]
            if (uri = (ENV['MONGOHQ_URL'] || ENV['MONGOLAB_URI']).to_s) && !uri.empty?
              ::Mongoid.clients[:default] = {:uri => uri, :max_retries_on_connection_failure => 4}
            else
              ::Mongoid.connect_to('qu')
            end
          end
          ::Mongoid::Clients.with_name(@client_name)
        end
      end
      alias_method :database, :connection

      # Pass in a symbol session identifier, or an actual client
      # TODO: verify this works when Threading
      def connection=(connection)
        if connection.respond_to? :to_sym
          @client_name = connection
          Thread.current[self.to_s] = nil
          connection
        else
          Thread.current[self.to_s] = connection
        end
      end

      def clear(queue = 'default')
        jobs(queue).drop
      end

      def size(queue = 'default')
        jobs(queue).count
      end

      def push(payload)
        payload.id = ::BSON::ObjectId.new
        jobs(payload.queue).insert_one(payload_attributes(payload))
        payload
      end

      def pop(queue = 'default')
        begin
          doc = jobs(queue).find_one_and_delete({})

          if doc
            doc['id'] = doc.delete('_id')
            return Payload.new(doc)
          end
        rescue ::Mongo::Error::OperationFailure => e
          # No jobs in the queue (MongoDB <2)
        end
      end

      def abort(payload)
        jobs(payload.queue).insert_one(payload_attributes(payload))
      end

    private

      def payload_attributes(payload)
        attrs = payload.attributes_for_push
        attrs[:_id] = attrs.delete(:id)
        attrs
      end

      def jobs(queue)
        connection["qu:queue:#{queue}"]
      end
    end
  end
end
