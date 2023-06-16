import os
import yaml

BASE_DIR = os.path.dirname(__file__)
CONFIG_DIR = os.path.expanduser('~/.config/ctadel')
CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.yaml')

class Conf:

    def __init__(self):
        self.default_config = {
                'config_file': 'config.yaml',
                'configurations': {
                        'console': False,
                        'rabbit_message_in_console': False,
                        'allow_open_input_file': False,
                        'theme': 'auto'
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

        if not os.path.exists(CONFIG_DIR):
            os.mkdir(CONFIG_DIR)

        if persistent:
            with open(CONFIG_FILE, 'w') as file:
                yaml.dump(config, file)

        self.config = config


    def get_console(self):
        try:
            return self.config['configurations']['console']
        except: return False

    def get_theme(self):
        try:
            theme = self.config['configurations']['theme']
            if theme not in {'auto', 'light', 'dark'}:
                print(f"Invalid theme type : {theme}")
                raise ValueError
            return theme

        except Exception as e:
            return "auto"

    def get_file_types(self):
        if self.config.get('db') and self.config['db'].get('file_type'):
            return self.config['db']['file_type']
        else: return []

    def get_company_names(self):
        if self.config.get('db') and self.config['db'].get('company'):
            return self.config['db']['company']
        else: return []

    def get_datatypes(self):
        if self.config.get('db') and self.config['db'].get('data_type'):
            return self.config['db']['data_type']
        else: return []

    def get_filesubtypes(self):
        if self.config.get('db') and self.config['db'].get('file_sub_type'):
            return self.config['db']['file_sub_type']
        else: return []

    def get_rabbit_queues(self):
        if self.config.get('rabbit') and self.config['rabbit'].get('rabbit_queue_name'):
            return self.config['rabbit']['rabbit_queue_name']
        else: return []

    def get_folder_name(self):
        if self.config.get('db') and self.config['db'].get('folder_name'):
            return self.config['db']['folder_name']
        else: return ''

    def get_db_name(self):
        if self.config.get('db') and self.config['db'].get('db_name'):
            return self.config['db']['db_name']
        else: return ''


    def get_rabbit_information(self):
        general_config = self.config.get('rabbit')

        if not general_config:
            return self.default_config['rabbit']
        else:
            self.default_config['rabbit'] \
                .update(self.config.get('rabbit'))
            return self.default_config['rabbit']


    def get_aws_information(self):
        general_config = self.config.get('aws')

        if not general_config:
            return self.default_config['aws']
        else:
            self.default_config['aws'] \
                .update(self.config.get('aws'))
            return self.default_config['aws']


    def get_general_config(self):
        general_config = self.config.get('configurations')

        if not general_config:
            return self.default_config['configurations']
        else:
            self.default_config['configurations'] \
                .update(self.config.get('configurations'))
            return self.default_config['configurations']

C = Conf()

