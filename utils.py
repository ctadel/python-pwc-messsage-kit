import os
import json
import pika
import boto3

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import *

def add_delete_move_functionality(list_widget, buttons):
    def add_item():
        dialog = QDialog()
        dialog.setWindowTitle('Add Item')

        layout = QVBoxLayout(dialog)
        display_name_label = QLabel('Enter display name:')
        display_name_edit = QLineEdit()
        value_label = QLabel('Enter value (optional):')
        value_edit = QLineEdit()
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)

        layout.addWidget(display_name_label)
        layout.addWidget(display_name_edit)
        layout.addWidget(value_label)
        layout.addWidget(value_edit)
        layout.addWidget(button_box)

        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)

        if dialog.exec_() == QDialog.Accepted:
            display_name = display_name_edit.text().strip()
            value = value_edit.text().strip()

            if display_name:
                item = QListWidgetItem(display_name)
                item.setData(Qt.UserRole, display_name)
                item.setData(Qt.UserRole + 1, value)
                list_widget.addItem(item)

    def delete_item():
        selected_items = list_widget.selectedItems()
        if selected_items:
            for item in selected_items:
                list_widget.takeItem(list_widget.row(item))

    def move_up():
        current_row = list_widget.currentRow()
        if current_row > 0:
            item = list_widget.takeItem(current_row)
            list_widget.insertItem(current_row - 1, item)
            list_widget.setCurrentRow(current_row - 1)

    def move_down():
        current_row = list_widget.currentRow()
        if current_row < list_widget.count() - 1:
            item = list_widget.takeItem(current_row)
            list_widget.insertItem(current_row + 1, item)
            list_widget.setCurrentRow(current_row + 1)

    add_button, move_up_button, move_down_button, delete_button = buttons

    # Connect buttons to their respective functions
    add_button.clicked.connect(add_item)
    delete_button.clicked.connect(delete_item)
    move_up_button.clicked.connect(move_up)
    move_down_button.clicked.connect(move_down)


def retrive_list_widget_items(listwidget):
    items = []
    for index in range(listwidget.count()):
        item = listwidget.item(index)
        name = item.text()
        value = item.data(Qt.UserRole)  # Assuming the value is stored using UserRole
        items.append((name, value))
    return items


class RabbitMQ:
    def __init__(self, config, db_name, queue_name):
        self.username = config['credentials']['username']
        self.password = config['credentials']['password']

        self.server_config = dict(
                host = config['server']['host'],
                port = config['server']['port'],
                virtual_host = config['server']['virtual_host']
            )

        self.queue_name = queue_name
        self.mongo_db = db_name
        self.connection = None

    def get_connection(self):
        credentials = pika.PlainCredentials(username=self.username, password=self.password)
        parameters = pika.ConnectionParameters(**self.server_config, credentials=credentials)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        return channel

    def publish(self, message:dict):

        if not self.connection:
            self.connection = self.get_connection()

        self.connection.queue_declare(queue=self.queue_name, durable=True)
        self.connection.basic_publish(
            exchange    =   '',
            routing_key =   self.queue_name,
            body        =   json.dumps(message),

            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                headers={'db_name': self.mongo_db}
            ))

    def __enter__(self):
        try:
            self.connection = self.get_connection()
            return self
        except:
            raise ConnectionError("Error connecting to Rabbit server")

    def __exit__(self, *_):
        self.connection.close()


class AWS:

    def __init__(self, aws_config, folder_name):

        self.bucket_name = aws_config['bucket_name']
        self.endpoint_url = aws_config['endpoint_url']
        self.access = aws_config['credentials']['aws_access_key_id']
        self.secret = aws_config['credentials']['aws_secret_access_key']

        self.folder_name = folder_name

    def get_connection(self):
        if not all([self.endpoint_url, self.access, self.secret, self.bucket_name]):
            raise ValueError("Incomplete aws configuration")

        try:
            self.client = boto3.client(
                    's3',
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=self.access,
                    aws_secret_access_key=self.secret
                )
        except:
            raise ConnectionError("Invalid AWS configuration")

    def list_buckets(self):
        self.get_connection()
        return self.client.list_buckets()

    def upload(self, local_file_path):

        self.get_connection()
        s3_file_path = os.path.join(
                self.folder_name,
                os.path.basename(local_file_path)
            )

        self.client.upload_file(local_file_path, self.bucket_name, s3_file_path)

