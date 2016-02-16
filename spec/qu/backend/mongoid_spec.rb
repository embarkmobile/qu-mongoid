require 'spec_helper'
require 'qu-mongoid'

describe Qu::Backend::Mongoid do
  it_should_behave_like 'a backend'
  it_should_behave_like 'a backend interface'

  describe 'connection' do
    it 'defaults to the qu database' do
      expect(subject.connection).to be_instance_of(Mongo::Client)
      expect(subject.connection.options[:database]).to eq('qu')
    end
    
    it 'defaults to the :default client' do
      expect(subject.client_name).to eq(:default)
      expect(subject.connection).to eq(::Mongoid::Clients.with_name(subject.client_name))
    end
    
    it 'has a configurable client that works with threads' do
      subject.connection = nil
      
      ::Mongoid.clients[:qu] = {:uri => 'mongodb://localhost:27017/quspec', :max_retries_on_connection_failure => 4}
      Qu.backend = subject
      Qu.configure do |c|
        c.backend.connection = :qu
      end
      expect(subject.connection).to eq(::Mongoid::Clients.with_name(:qu))
      
      should_have_qu_session_in_new_thread = false
      Thread.new do
        should_have_qu_session_in_new_thread = (subject.connection == ::Mongoid::Clients.with_name(:qu))
      end.join
      expect(should_have_qu_session_in_new_thread).to be true
      
      # Clean up
      subject.connection=nil
      ::Mongoid.connect_to('qu')
    end
    
    it 'uses MONGOHQ_URL from heroku' do
      # Clean up from other tests
      ::Mongoid.clients[:default]  = nil
      subject.connection = nil
      ::Mongoid::Clients.clear
      
      ENV['MONGOHQ_URL'] = 'mongodb://127.0.0.1:27017/quspec'
      expect(subject.connection.options[:database]).to eq('quspec')

      node = subject.connection.cluster.servers.first

      expect(node.address.to_s).to eq("127.0.0.1:27017")

      # expect(::Mongoid.clients[:default][:hosts]).to include("127.0.0.1:27017")
      
      # Clean up MONGOHQ stuff
      ENV.delete('MONGOHQ_URL')
      subject.connection=nil
      ::Mongoid.connect_to('qu')
    end
  end

  describe 'pop' do
    let(:worker) { Qu::Worker.new }

    describe "on mongo >=2" do
      it 'returns nil when no jobs exist' do
        subject.clear
        expect_any_instance_of(Mongo::Collection).to receive(:find_one_and_delete).and_return(nil)
        expect { expect(subject.pop(worker)).to be_nil }.not_to raise_error
      end
    end

    describe 'on mongo <2' do
      it 'returns nil when no jobs exist' do
        subject.clear
        expect_any_instance_of(Mongo::Collection).to receive(:find_one_and_delete).and_raise(Mongo::Error::OperationFailure.new('test'))
        expect { expect(subject.pop(worker)).to be_nil }.not_to raise_error
      end
    end
  end
end
