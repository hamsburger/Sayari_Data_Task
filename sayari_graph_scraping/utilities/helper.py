import yaml

def read_spider_configs(file_path):
    with open(file_path) as stream:
        yaml_file = yaml.safe_load(stream)
    
    return yaml_file
