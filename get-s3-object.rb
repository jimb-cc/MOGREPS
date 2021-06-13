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



bucket_name = 'aws-earth-mo-atmospheric-global-prd'
object_key = '00d4ece55c3f2330f9a90879d25e0964df242ab1.nc'
local_path = "./files/#{object_key}"
region = opts[:awsregion]
s3_client = Aws::S3::Client.new(region: region)




  def object_downloaded?(s3_client, bucket_name, object_key, local_path)
    s3_client.get_object(
      response_target: local_path,
      bucket: bucket_name,
      key: object_key
    )
  rescue StandardError => e
    puts "Error getting object: #{e.message}"
  end

  if object_downloaded?(s3_client, bucket_name, object_key, local_path)
    puts "Object '#{object_key}' in bucket '#{bucket_name}' " \
      "downloaded to '#{local_path}'."
  else
    puts "Object '#{object_key}' in bucket '#{bucket_name}' not downloaded."
  end







# Downloads an object from an Amazon Simple Storage Service (Amazon S3) bucket.
#
# @param s3_client [Aws::S3::Client] An initialized S3 client.
# @param bucket_name [String] The name of the bucket containing the object.
# @param object_key [String] The name of the object to download.
# @param local_path [String] The path on your local computer to download
#   the object.
# @return [Boolean] true if the object was downloaded; otherwise, false.
# @example
#   exit 1 unless object_downloaded?(
#     Aws::S3::Client.new(region: 'us-east-1'),
#     'doc-example-bucket',
#     'my-file.txt',
#     './my-file.txt'
#   )
