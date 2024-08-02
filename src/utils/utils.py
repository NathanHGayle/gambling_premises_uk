import yaml


def find_from_config(key, name):
    with open('../config/old_config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config[key][name]
