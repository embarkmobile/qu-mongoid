source "http://rubygems.org"

# Specify your gem's dependencies in qu-mongoid.gemspec
gemspec

gem 'mongoid', '~> 5.0.0'
gem 'mongo', '~> 2.1.2'

group :test do
  gem 'SystemTimer',  :platform => :mri_18
end

group :development, :test do
  gem 'qu', git: 'https://github.com/bkeepers/qu.git'
end
