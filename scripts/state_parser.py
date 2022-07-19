import boto3
import configparser
import logging
import re
import os
from jinja2 import Environment, FileSystemLoader

def parse_bucket_objects(bucket_name, search_string):
  """
  Returns a list of bucket objects that match a given string.

    Parameters:
      bucket_name (str): canonical name of s3 bucket.
      search_string (str): string to match objects with.
    
    Returns:
      matching_objects (list): list of matching object keys.
  """
  matching_objects = list()
  regex_pattern = r"^" + re.escape(search_string)

  for bucket_object in bucket_name.objects.all():
      if re.match(regex_pattern, bucket_object.key):
        matching_objects.append(bucket_object.key)
      else:
        logging.info("Key doesn't match project: {}".format(bucket_object.key))
  
  return matching_objects

def parse_res_classes(state_file):
  """
  Returns a list of res_classes, by taking the state file name and stripping
  out the res_class.

    Parameters:
      state_file (list): a list of state file names.
    
    Returns:
      res_class_name (str): list of res_class names.
  """
  remove_suffix = re.sub('\.tfstate$', '', state_file)
  res_class_name = (remove_suffix.split('-',2)[2])
  
  return res_class_name


def generate_backend_file(bucket_name, state_file_list, aws_region, project_environment):
  file_loader = FileSystemLoader('templates')
  env = Environment(loader=file_loader)

  template = env.get_template('backend.j2')

  for state_file in state_file_list:
    res_class = parse_res_classes(state_file)
    output_dir = "./generated/" + project_environment + "/" + res_class + "/"
    output_file_name = "backend.tf"
    output_uri = output_dir + output_file_name

    if not os.path.exists(output_dir):
      os.makedirs(output_dir)
    # backend_file = template.render(
    #   bucket_name=bucket_name, 
    #   state_file=state_file,
    #   region=aws_region)

    template.stream(
      bucket=bucket_name, 
      key=state_file,
      region=aws_region).dump(output_uri)


def main():
  parser = configparser.ConfigParser()
  parser.read("config.txt")

  aws_region = parser.get("aws", "region")
  project_environment = parser.get("project", "environment")
  bucket_name = parser.get("S3", "bucket_name")
  logging_level = parser.get("logging", "level")

  logging.basicConfig(
    filename='state_parser.log', 
    filemode='w', 
    level=logging_level)

  s3 = boto3.resource('s3')
  state_bucket = s3.Bucket(bucket_name)

  state_file_list = list()
  state_file_list = parse_bucket_objects(state_bucket, project_environment)

  generate_backend_file(
    bucket_name, 
    state_file_list, 
    aws_region, 
    project_environment)


if __name__ == "__main__":
    main()