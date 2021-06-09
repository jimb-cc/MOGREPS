# frozen_string_literal: true

require 'mongo'
require 'slop'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'

opts = Slop.parse do |o|
  o.string '-h', '--host', 'connection string (default: localhost)', default: 'mongodb://localhost'
  o.string '-d', '--database', 'the database to use (default: hs2)', default: 'mogreps'
  o.string '-c', '--collection', 'the collection to use (default: p1)', default: 'test'
  o.string '-r', '--awsregion', 'the aws region (default: eu-west-2)', default: 'eu-west-2'
  o.string '-q', '--sqsURL', 'the aws SQS url (default: none)', default: ''
end

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN
puts "## Connecting to #{opts[:host]}, and db #{opts[:database]}"
DB = Mongo::Client.new(opts[:host], database: opts[:database])

DB[opts[:collection]].drop

def db_insert(_message, db)
  message = JSON.parse(_message)
  submessage = JSON.parse(message['Message'])
  message['messageAsJSON'] = submessage
  result = db.insert_one(message)
  puts "# inserted into DB #{result} NetCDF in bucket #{submessage['bucket']} with key #{submessage['key']} and a size of #{submessage['object_size']}"
end

def receive_messages(sqs_client, queue_url, max_number_of_messages = 10, db)
  response = sqs_client.receive_message(
    queue_url: queue_url,
    max_number_of_messages: max_number_of_messages
  )

  if response.messages.count.zero?
    puts 'No messages to receive, or all messages have already ' \
      'been previously received.'
    return
  end

  response.messages.each do |message|
    db_insert(message.body.to_s, db)
  end
rescue StandardError => e
  puts "Error receiving messages: #{e.message}"
end

def get_messages(db, queue_url, region)
  max_number_of_messages = 10
  sqs_client = Aws::SQS::Client.new(region: region)
  receive_messages(sqs_client, queue_url, max_number_of_messages, db)
end

get_messages(DB[opts[:collection]], opts[:sqsURL], opts[:awsregion])
