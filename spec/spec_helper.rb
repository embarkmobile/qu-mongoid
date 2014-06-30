require 'bundler'
Bundler.require :test
require 'qu'
require 'qu/backend/spec'

root_path = Pathname(__FILE__).dirname.join('..').expand_path

RSpec.configure do |config|
  config.before(:each) do
    Qu.backend = double('a backend', {
      :push => nil,
      :pop => nil,
      :complete => nil,
      :abort => nil,
      :fail => nil,
    })
  end
end

log_path = root_path.join("log")
log_path.mkpath
log_file = log_path.join("qu.log")
log_to = ENV.fetch("QU_LOG_STDOUT", false) ? STDOUT : log_file

Qu.logger = Logger.new(log_to)