# frozen_string_literal: true

require 'mongo'
require 'slop'
require 'aws-sdk-s3'

opts = Slop.parse do |o|
  o.string '-h', '--host', 'connection string (default: localhost)', default: 'mongodb://localhost'
  o.string '-d', '--database', 'the database to use (default: hs2)', default: 'mogreps'
  o.string '-c', '--collection', 'the collection to use (default: p1)', default: 'test'
  o.string '-r', '--awsregion', 'the aws region (default: eu-west-2)', default: 'eu-west-2'
  o.string '-b', '--bucket', 'the aws s3 bucket (default: none)', default: ''
  o.string '-o', '--maxobjects', 'number of objects to list (default: 1000)', default: '1000'
end

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN
puts "## Connecting to #{opts[:host]}, and db #{opts[:database]}"
DB = Mongo::Client.new(opts[:host], database: opts[:database])

# DB[opts[:collection]].drop
begin
  puts '--- Creating Index'
  DB[opts[:bucket]].indexes.create_one({ key: 1 }, unique: true, name: 'ix_key', expire_after: 345_600)
rescue StandardError => e
  puts "Index probably already exists -> #{e}"
end

def listbuckets(s3_client, bucket_name, max_objects, db)
  objects = s3_client.list_objects_v2(
    bucket: bucket_name,
    max_keys: max_objects
  ).contents

  if objects.count.zero?
    puts "No objects in bucket '#{bucket_name}'."
    nil
  else
    objects.each do |object|
      result = db.insert_one(object.to_hash)
      puts "---- #{object.key} inserted into DB "
    rescue StandardError => e
      puts "---- Probably a dupe ---> #{object.key}"
    end
  end
rescue StandardError => e
  puts "Error accessing bucket '#{bucket_name}' " \
    "or listing its objects: #{e.message}"
end

s3_client = Aws::S3::Client.new(region: opts[:awsregion])

listbuckets(s3_client, opts[:bucket], opts[:maxobjects], DB[opts[:bucket]])
