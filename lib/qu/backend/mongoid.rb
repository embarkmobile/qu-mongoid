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
      
      attr_accessor :session

      def initialize
        self.max_retries     = 5
        self.retry_frequency = 1
        self.poll_frequency  = 5
        self.session = :default
      end

      def connection
        Thread.current[self.to_s] ||= begin
          unless ::Mongoid.sessions[@session]
            if (uri = (ENV['MONGOHQ_URL'] || ENV['MONGOLAB_URI']).to_s) && !uri.empty?
              ::Mongoid.sessions = {:default => {:uri => uri, :max_retries_on_connection_failure => 4}}
            else
              ::Mongoid.connect_to('qu')
            end
          end
          ::Mongoid::Sessions.with_name(@session)
        end
      end
      alias_method :database, :connection

      # Pass in a symbol session identifier, or an actual session
      # TODO: verify this works when Threading
      def connection=(connection)
        if connection.respond_to? :to_sym
          @session = connection
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
        jobs(queue).find.count
      end

      if defined?(::Moped::BSON::ObjectId)
        def new_id
          ::Moped::BSON::ObjectId.new
        end
      else
        def new_id
          ::BSON::ObjectId.new
        end
      end
      private :new_id

      def push(payload)
        payload.id = new_id
        jobs(payload.queue).insert(payload_attributes(payload))
        payload
      end

      def pop(queue = 'default')
        begin
          doc = jobs(queue).find.modify({}, remove: true)

          if doc
            doc['id'] = doc.delete('_id')
            return Payload.new(doc)
          end
        rescue ::Moped::Errors::OperationFailure => e
          # No jobs in the queue (MongoDB <2)
        end
      end

      def abort(payload)
        jobs(payload.queue).insert(payload_attributes(payload))
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
