"""
                            NOTE

This used functions from the pwc repo so this should be
placed into the repo directory to be used as a publisher.


"""

import re
import os
import sys
import json
from argparse import ArgumentParser, REMAINDER

import pika
import boto3

from config import config
from constants.constant import RABBITMQ_QUEUE_NAME

def setup_parser(parser:ArgumentParser):
    parser.add_argument(
            'file_name', type=str, action='store',
            help="Location of the file that is to be uploaded and published."
        )

    parser.add_argument(
            '--silent', action='store_true', default=False,
            help="Pass this argument if you dont want any message to be displayed"
        )

    parser.add_argument(
            '--show', action='store_true', default=False,
            help="View the partial content of the dataframe"
        )

    parser.add_argument(
            '-T', '--file_type', action='store', required=True,
            help="'file_type' value in the to be published message"
        )

    parser.add_argument(
            '-C', '--company_name', action='store', default='generic',
            help="company name such as 'generic', 'reload', 'tml'"
        )

    parser.add_argument(
            '-D', '--data_type', action='store', default='',
            help="'data_type' value in the to be published message"
        )

    parser.add_argument(
            '-B', '--bucket_name', action='store', default=config.BUCKET_NAME,
            help="The bucket name for the file upload/download"
        )

    parser.add_argument(
            '-L', '--load_id', type=int, action='store', default='1',
            help="The load_id value for the message"
        )

    parser.add_argument(
            '-SF', '--file_sub_type', action='store', default='',
            help="The file_sub_type value for the message"
        )

    parser.add_argument(
            '-OF', '--original_file_name', action='store', default='',
            help="The original_file_name value for the message"
        )

    parser.add_argument(
            '-F', '--folder_name', action='store', default='',
            help="The original_file_name value for the message"
        )
    parser.add_argument(
            '-LT', '--loadType', action='store', default='',
            help="The loadType value for the message"
        )

    parser.add_argument(
            '-X', '--debug', action='store_true', default=False,
            help="Passing this argument will add a breakpoint in the script"
        )

    # Additional user-defined arguments
    parser.add_argument('additional_args', nargs=REMAINDER, help="Additional user-defined arguments")

    return parser


def process_parser_args(args):
    data = vars(args).copy()
    additional_args = data.pop('additional_args')
    for arg in additional_args:
        new_arg = re.match(r'--(\w*)=(.*)', arg)

        if new_arg:
            data[new_arg.groups()[0]] = new_arg.groups()[1]
        else:
            print(f"**Argument {arg} is invalid and was ignored**")

    # these are not the part of actual message but were
    # only used in parser arguements, hence we delete
    del data['silent']
    del data['show']
    del data['debug']

    return data


class RabbitMQ:
    def __init__(self):
        self.username = config.RABBITMQ_USERNAME
        self.password = config.RABBITMQ_PASSWORD
        self.queue_name = RABBITMQ_QUEUE_NAME

        self.server_config = dict(
                host = config.RABBITMQ_HOST,
                port = config.RABBITMQ_PORT,
                virtual_host = config.RABBITMQ_VIRTUAL_HOST,
            )

        self.mongo_db = config.MONGO_DB
        self.connection = None

    def get_connection(self):
        credentials = pika.PlainCredentials(username=self.username, password=self.password)
        parameters = pika.ConnectionParameters(**self.server_config, credentials=credentials)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=True)

        return channel

    def publish(self, message:dict):

        if not self.connection:
            self.connection = self.get_connection()

        self.connection.basic_publish(
            exchange    =   '',
            routing_key =   self.queue_name,
            body        =   json.dumps(message),

            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                headers={'db_name': self.mongo_db}
            ))

    def __enter__(self):
        self.connection = self.get_connection()
        return self

    def __exit__(self, *_):
        self.connection.close()


class AWS:

    def __init__(self, **kwargs):

        self.local_file_path = kwargs.get('file_name')
        self.bucket_name = kwargs.get('bucket_name')
        self.folder_name = kwargs.get('folder_name')

        self.s3_path = os.path.join(
                self.folder_name,
                os.path.basename(self.local_file_path)
            )

    def __enter__(self):

        self.client = boto3.client(
                's3',
                endpoint_url=config.AWS_URL,
                aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
            )

        return self

    def __exit__(self, *_):
        ...


def read_file_helper(message):

    file_name = message['file_name']
    extension = os.path.splitext(file_name)[1]

    with open(file_name, 'rb') as file:
        file_data = file.read()

    return extension, file_data, message, file_name



if __name__ == "__main__":

    parser = ArgumentParser(
            prog="PWC Python Message Kit",
            description="Upload Publish and Test"
        )

    #step 1 : process message
    args = setup_parser(parser).parse_args()
    message = process_parser_args(args)
    print("\n>>>>>>>>>>>>>>>>>>>>>>>> PWC Python Message Kit <<<<<<<<<<<<<<<<<<<<<<<<\n")


    if not os.path.exists(args.file_name):
        print(f"❌ INVALID FILE PATH: {args.file_name}\n")
        sys.exit(1)

    if args.debug:
        __import__('pdb').set_trace()

    if args.show:
        try:
            from api.services.transformation.read_file import ReadFile
            df = ReadFile(*read_file_helper(message)).get_data()
            print(df, '\n\n')

        except Exception as e:
            print(f"Error while printing the dataframe: \n{e}")


    if args.silent:
        print = lambda *_ : None


    #step 2: upload file to aws/minio
    with AWS(**message) as aws:
        try:
            aws.client.upload_file(aws.local_file_path, aws.bucket_name, aws.s3_path)
            print(f'✅ File uploaded to the bucket: {aws.bucket_name}')

        except boto3.exceptions.S3UploadFailedError as e:
            print(f"❌ Error in Upload: {e}")
            sys.exit(2)


    #step 3: send message to the queue
    with RabbitMQ() as rmq:

        message['file_name'] = os.path.basename(message['file_name'])

        try:
            rmq.publish(message)
            print(f'✅ Message published to rabbitmq\n')
            print(json.dumps(message, indent=4))

        except Exception as e:
            print(f'❌ Error in rabbitmq: {e}')


    print("\n........................................................................\n")
