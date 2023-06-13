import os
import yaml

BASE_DIR = os.path.dirname(__file__)

CONFIG_FILE = os.path.join(BASE_DIR, 'config.yaml')

class Conf:

    def __init__(self):
        self.default_config = {
                'config_file': 'config.yaml',
                'configurations': {
                        'console': False,
                        'rabbit_message_in_console': False,
                        'allow_open_input_file': False,
                        'theme': 'dark'
                    },
                'db': {
                        'company': [],
                        'file_type': [],
                        'data_type': [],
                        'file_sub_type': [],
                        'db_name': '',
                        'folder_name': ''
                    },
                'rabbit': {
                        'rabbit_queue_name': [],
                        'credentials': {
                                'username': 'guest',
                                'password': 'guest'
                             },
                        'server': {
                                'host': '127.0.0.1',
                                'port': '14567',
                                'virtual_host': '/'
                            }
                    },
                'aws': {
                        'bucket_name': '',
                        'endpoint_url': '',
                        'credentials': {
                                'aws_access_key_id':'',
                                'aws_secret_access_key':''
                            }
                    }
            }

        self.config = self.read_config()

    def read_config(self):
        try:
            with open(CONFIG_FILE, 'r') as file:
                config = yaml.full_load(file)
        except:
            print('Error while reading the config file, loading default')
            config = self.default_config
        return config


    def write_config(self, config:dict, persistent=False):

        if persistent:
            with open(CONFIG_FILE, 'w') as file:
                yaml.dump(config, file)

        self.config = config


    def get_console(self):
        try:
            return self.config['configurations']['console']
        except Exception as e:
            print(e)
            return False

    def get_file_types(self):
        try:
            return self.config['db']['file_type']
        except Exception as e:
            print(e)
            return []

    def get_company_names(self):
        try:
            return self.config['db']['company']
        except Exception as e:
            print(e)
            return []

    def get_datatypes(self):
        try:
            return self.config['db']['data_type']
        except Exception as e:
            print(e)
            return []

    def get_filesubtypes(self):
        try:
            return self.config['db']['file_sub_type']
        except Exception as e:
            print(e)
            return []

    def get_rabbit_queues(self):
        try:
            return self.config['rabbit']['rabbit_queue_name']
        except Exception as e:
            print(e)
            return []

    def get_folder_name(self):
        try:
            return self.config['db']['folder_name']
        except Exception as e:
            print(e)
            return ''

    def get_db_name(self):
        try:
            return self.config['db']['db_name']
        except Exception as e:
            print(e)
            return ''

    def get_rabbit_information(self):

        try:
            return self.config['rabbit']
        except Exception as e:
            print(e)
            return {}

    def get_aws_information(self):
        try:
            return self.config['aws']
        except Exception as e:
            print(e)
            return {}

    def get_general_config(self):
        try:
            return self.config['configurations']
        except Exception as e:
            print(e)
            return self.default_config['configurations']

C = Conf()

