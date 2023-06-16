import os
import json
import sys
import subprocess
from time import sleep
from threading import Thread

import qdarktheme
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt, QPropertyAnimation, QSettings, QPoint
from PyQt5 import uic

import utils
from config import C, BASE_DIR

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()

        # get and set last remebered app position
        settings = QSettings("ctadel", "pwc-message-kit")
        pos = settings.value("window_position", QPoint(200, 200))
        self.move(pos)

        uic.loadUi(os.path.join(BASE_DIR, "resources", "main_window.ui"), self)
        self.apply_configuration()
        self.show()

        self.general_config = C.get_general_config()

        # Menu Operations
        self.actionConsole.triggered.connect(self.toggle_console)
        self.actionConfiguration.triggered.connect(self.open_config_window)
        self.actionAbout.triggered.connect(self.open_about_window)
        self.menuAbout.addAction(self.actionAbout)
        self.actionExit.triggered.connect(sys.exit)

        self.btn_browse.clicked.connect(self.browse_input_file)
        self.btn_execute.clicked.connect(self.open_selected_file)
        self.btn_run.clicked.connect(self.run)
        self.btn_clear.clicked.connect(
                lambda: self.console.clear()
            )

        self.btn_execute.setVisible(self.general_config['allow_open_input_file'])
        self.btn_execute.setEnabled(False)
        self.btn_run.setEnabled(False)

    def toggle_console(self):
        if self.actionConsole.isChecked():
            self.setFixedWidth(650)
        else:
            self.setFixedWidth(310)


    def delete_old_configuration(self):
        self.x_filetype.clear()
        self.x_company.clear()
        self.x_datatype.clear()
        self.x_filetype.clear()
        self.x_rabbit_queue.clear()


    def apply_configuration(self):
        self.console.append("Applying configurations")
        self.actionConsole.setChecked(C.get_console())

        for item in C.get_company_names():
            self.x_company.addItem(*item)
            self.x_company.setCurrentIndex(0)

        for item in C.get_file_types():
            self.x_filetype.addItem(*item)
            self.x_filetype.setCurrentIndex(0)

        for item in C.get_datatypes():
            self.x_datatype.addItem(*item)
            self.x_datatype.setCurrentIndex(0)

        for item in C.get_filesubtypes():
            self.x_filesubtype.addItem(*item)
            self.x_filesubtype.setCurrentIndex(0)

        for item in C.get_rabbit_queues():
            self.x_rabbit_queue.addItem(*item)
            self.x_rabbit_queue.setCurrentIndex(0)

        self.x_foldername.setPlainText(C.get_folder_name())
        self.x_dbname.setPlainText(C.get_db_name())
        self.x_bucketname.setPlainText(C.get_aws_information().get('bucket_name'))

        self.toggle_console()
        self.console.append("Loaded configurations")

    def open_config_window(self):
        config_window = ConfigWindow(self)
        config_window.exec_()

    def open_about_window(self):
        aboutwindow = AboutWindow(self)
        aboutwindow.exec_()

    def open_selected_file(self):
        filename = self.x_inputfile.toPlainText()

        if filename.split('.')[-1] not in {'xls','xlsx','csv','txt','odt'}:
            self.console.append("Cannot open file of these sorts.")
            return

        subprocess.Popen(['libreoffice', filename])


    def browse_input_file(self):
        filename = QFileDialog.getOpenFileName(self, "Select File", os.path.expanduser('~/Downloads/'))[0]
        self.x_inputfile.setPlainText(filename)

        self.btn_execute.setEnabled(bool(filename))
        self.btn_run.setEnabled(bool(filename))


    def run(self):

        progress_animation = QPropertyAnimation(self.progressBar, b"value")
        progress_animation.setDuration(1000)

        progress_animation.setStartValue(0)
        progress_animation.setEndValue(20)
        progress_animation.start()  # Start the animation

        aws = utils.AWS(C.get_aws_information(), C.get_folder_name())
        try:
            aws.upload(self.x_inputfile.toPlainText())
            self.console.append(
                    f'✅ File {self.x_inputfile.toPlainText()} was uploaded to {C.get_aws_information().get("bucket_name")}'
                )

        except Exception as e:

            progress_animation.setStartValue(20)
            progress_animation.setEndValue(30)
            progress_animation.start()  # Start the animation

            if isinstance(e, ValueError):
                self.open_config_window()

            self.console.append(f"❌ AWS Exception: {e}")
            return

        progress_animation.setStartValue(0)
        progress_animation.setEndValue(50)
        progress_animation.start()  # Start the animation


        try:
            with utils.RabbitMQ(
                    C.get_rabbit_information(),
                    C.get_db_name(),
                    self.x_rabbit_queue.currentData()
                ) as rmq:

                message = dict(
                        file_name = os.path.basename(self.x_inputfile.toPlainText()),
                        company_name = self.x_company.currentData(),
                        file_type = self.x_filetype.currentData(),
                        data_type = self.x_datatype.currentData(),
                        load_id = self.x_loadid.value(),
                        file_sub_type = self.x_filesubtype.currentData(),
                        bucket_name = self.x_bucketname.toPlainText(),
                        folder_name = self.x_foldername.toPlainText(),
                        original_file_name = self.x_originalfilename.toPlainText(),
                    )

                try:
                    rmq.publish(message)
                    self.console.append(f'✅ Message published to rabbitmq\n')

                    if self.general_config['rabbit_message_in_console']:
                        self.console.append(json.dumps(message, indent=4))

                    progress_animation.setStartValue(50)
                    progress_animation.setEndValue(100)
                    progress_animation.start()  # Start the animation

                except Exception as e:
                    progress_animation.setStartValue(50)
                    progress_animation.setEndValue(80)
                    progress_animation.start()  # Start the animation
                    self.console.append(f'❌ Error while publishing to rabbitmq: {e}')

            self.progressBar.setValue(100)
        except Exception as e:
            self.console.append(f"Exception: {e}")
            self.progressBar.setValue(0)


    def closeEvent(self, event):
        # Save the window position in settings when the application is closed
        settings = QSettings("ctadel", "pwc-message-kit")
        settings.setValue("window_position", self.pos())

        super().closeEvent(event)


class ConfigWindow(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.parent = parent
        uic.loadUi(os.path.join(BASE_DIR, "resources", "conf_window.ui"), self)
        self.show()

        self.btn_save.clicked.connect(lambda: self.save())
        self.btn_ignore.clicked.connect(self.discard)
        self.btn_restore.clicked.connect(self.restore_defaults_clicked)

        self.theme_auto.toggled.connect(self.update_theme)
        self.theme_light.toggled.connect(self.update_theme)
        self.theme_dark.toggled.connect(self.update_theme)

        self.rabbit_test.clicked.connect(self.test_rabbit_connection)
        self.aws_test.clicked.connect(self.test_aws_connection)

        self.load_configurations()

    def load_configurations(self):
        self.checkBox_console.setChecked(C.get_console())
        self.cb_rabbit_message_in_console.setChecked(C.get_general_config()['rabbit_message_in_console'])
        self.cb_allow_open_input_file.setChecked(C.get_general_config()['allow_open_input_file'])
        self.get_config_theme().setChecked(True)

        for item in C.get_company_names():
            self.add_item(self.listWidget_company, item)

        utils.add_delete_move_functionality(self.listWidget_company,
                (self.cn_add, self.cn_up, self.cn_down, self.cn_delete)
            )

        for item in C.get_file_types():
            self.add_item(self.listWidget_filetype, item)

        utils.add_delete_move_functionality(self.listWidget_filetype,
                (self.ft_add, self.ft_up, self.ft_down, self.ft_delete)
            )

        for item in C.get_datatypes():
            self.add_item(self.listWidget_datatype, item)

        utils.add_delete_move_functionality(self.listWidget_datatype,
                (self.dt_add, self.dt_up, self.dt_down, self.dt_delete)
            )

        for item in C.get_filesubtypes():
            self.add_item(self.listWidget_filesubtype, item)

        utils.add_delete_move_functionality(self.listWidget_filesubtype,
                (self.st_add, self.st_up, self.st_down, self.st_delete)
            )

        for item in C.get_rabbit_queues():
            self.add_item(self.listWidget_rabbitqueues, item)

        utils.add_delete_move_functionality(self.listWidget_rabbitqueues,
                (self.rq_add, self.rq_up, self.rq_down, self.rq_delete)
            )


        self.config_foldername.setPlainText(C.get_folder_name())
        self.config_dbname.setPlainText(C.get_db_name())

        self.setup_rabbit()
        self.setup_aws()


    def setup_aws(self):
        self.aws = utils.AWS(C.get_aws_information(), C.get_folder_name())
        self.aws_endpoint.setPlainText(self.aws.endpoint_url)
        self.aws_access.setPlainText(self.aws.access)
        self.aws_secret.setPlainText(self.aws.secret)
        self.aws_bucket.setPlainText(self.aws.bucket_name)

    def setup_rabbit(self):
        self.rmq = utils.RabbitMQ(
                C.get_rabbit_information(),
                C.get_db_name(),
                self.parent.x_rabbit_queue.currentData()
            )
        self.rabbit_username.setPlainText(self.rmq.username)
        self.rabbit_password.setPlainText(self.rmq.password)
        self.rabbit_host.setPlainText(self.rmq.server_config['host'])
        self.rabbit_port.setPlainText(self.rmq.server_config['port'])
        self.rabbit_vhost.setPlainText(self.rmq.server_config['virtual_host'])

    def revert_styled_button(self, button):
        sleep(1.5)
        button.setStyleSheet("background-color: grey; color: black; border: 1px solid grey;")


    def test_rabbit_connection(self):
        try:
            self.save(persistent=False)
            self.setup_rabbit()
            connection = self.rmq.get_connection()
            connection.close()
            self.rabbit_test.setStyleSheet("background-color: green; border: 2px solid green;")
            self.rabbit_test.setAutoFillBackground(True)
            effect = QGraphicsDropShadowEffect(self.rabbit_test)
            effect.setColor(Qt.green)
            effect.setOffset(0, 0)
            effect.setBlurRadius(20)
            self.rabbit_test.setGraphicsEffect(effect)
            self.parent.console.append("Connection to RabbitMQ was successfull")
        except Exception as e:
            self.rabbit_test.setStyleSheet("background-color: red; border: 2px solid red;")
            self.rabbit_test.setAutoFillBackground(True)
            effect = QGraphicsDropShadowEffect(self.rabbit_test)
            effect.setColor(Qt.red)
            effect.setOffset(0, 0)
            effect.setBlurRadius(20)
            self.rabbit_test.setGraphicsEffect(effect)
            self.parent.console.append("Error connecting to RabbitMQ")

        finally:
            Thread(target=self.revert_styled_button, args=(self.rabbit_test,)).start()

    def test_aws_connection(self):
        try:
            self.save(persistent=False)
            self.setup_aws()
            self.aws.list_buckets()
            self.aws_test.setStyleSheet("background-color: green; border: 2px solid green;")
            self.aws_test.setAutoFillBackground(True)
            effect = QGraphicsDropShadowEffect(self.aws_test)
            effect.setColor(Qt.green)
            effect.setOffset(0, 0)
            effect.setBlurRadius(20)
            self.aws_test.setGraphicsEffect(effect)
            self.parent.console.append("Connection to AWS S3/Minio was successfull")
        except Exception as e:
            print(e)
            self.aws_test.setStyleSheet("background-color: red; border: 2px solid red;")
            self.aws_test.setAutoFillBackground(True)
            effect = QGraphicsDropShadowEffect(self.aws_test)
            effect.setColor(Qt.red)
            effect.setOffset(0, 0)
            effect.setBlurRadius(20)
            self.aws_test.setGraphicsEffect(effect)
            self.parent.console.append("Error connecting to AWS/Minio")

        finally:
            Thread(target=self.revert_styled_button, args=(self.aws_test,)).start()


    def add_item(self, list_widget, data):
        name, value = data
        item = QListWidgetItem(name)
        item.setData(Qt.UserRole, value)
        list_widget.addItem(item)

    def get_selected_theme(self):
        light = self.theme_light.isChecked()
        dark = self.theme_dark.isChecked()
        return 'light' if light else 'dark' if dark else 'auto'

    def get_config_theme(self):
        return dict(
                auto = self.theme_auto,
                light = self.theme_light,
                dark = self.theme_dark
            ).get(C.get_theme())

    def update_theme(self):
        qdarktheme.setup_theme(self.get_selected_theme())


    def save(self, persistent=True):
        data = C.default_config.copy()

        configurations = data['configurations']
        configurations['console'] = self.checkBox_console.isChecked()
        configurations['rabbit_message_in_console'] = self.cb_rabbit_message_in_console.isChecked()
        configurations['allow_open_input_file'] = self.cb_allow_open_input_file.isChecked()
        configurations['theme'] = self.get_selected_theme()

        data_variables = data['db']
        data_variables['file_type'] = utils.retrive_list_widget_items(self.listWidget_filetype)
        data_variables['company'] = utils.retrive_list_widget_items(self.listWidget_company)
        data_variables['data_type'] = utils.retrive_list_widget_items(self.listWidget_datatype)
        data_variables['file_sub_type'] = utils.retrive_list_widget_items(self.listWidget_filesubtype)
        data_variables['folder_name'] = self.config_foldername.toPlainText()
        data_variables['db_name'] = self.config_dbname.toPlainText()

        rabbit = data['rabbit']
        rabbit['credentials']['username'] = self.rabbit_username.toPlainText()
        rabbit['credentials']['password'] = self.rabbit_password.toPlainText()
        rabbit['server']['host'] = self.rabbit_host.toPlainText()
        rabbit['server']['port'] = self.rabbit_port.toPlainText()
        rabbit['server']['virtual_host'] = self.rabbit_vhost.toPlainText()
        rabbit['rabbit_queue_name'] = utils.retrive_list_widget_items(self.listWidget_rabbitqueues)

        aws = data['aws']
        aws['bucket_name'] = self.aws_bucket.toPlainText()
        aws['endpoint_url'] = self.aws_endpoint.toPlainText()
        aws['credentials']['aws_access_key_id'] = self.aws_access.toPlainText()
        aws['credentials']['aws_secret_access_key'] = self.aws_secret.toPlainText()

        C.write_config(data, persistent=persistent)

        if persistent:
            self.parent.console.append('Configurations saved..')
            self.parent.delete_old_configuration()
            self.parent.apply_configuration()
            self.close()


    def discard(self):
        self.close()

    def restore_defaults_clicked(self):
        reply = QMessageBox.question(self, "Confirmation", "Are you sure you want to restore the defaults?", QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.restore_defaults()

    def restore_defaults(self):
        self.parent.console.append('Settings restored to default')
        C.write_config(C.default_config.copy(), persistent=True)
        self.close()


class AboutWindow(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.parent = parent
        uic.loadUi(os.path.join(BASE_DIR, "resources", "about_window.ui"), self)
        self.show()


def main():
    app = QApplication([])
    app.setWindowIcon(QIcon(os.path.join(BASE_DIR, "resources", "icon.svg")))
    qdarktheme.setup_theme(C.get_theme())
    window = MainWindow()
    app.exec_()

if __name__ == "__main__":
    main()
