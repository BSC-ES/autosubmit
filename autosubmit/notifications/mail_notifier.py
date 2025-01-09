#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import smtplib
import zipfile
import tempfile
import email.utils
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from log.log import Log
from pathlib import Path
from autosubmitconfigparser.config.basicconfig import BasicConfig


class MailNotifier:
    def __init__(self, basic_config):
        self.config = basic_config

    def notify_experiment_status(self, exp_id, mail_to, platform):
        message_text = self._generate_message_experiment_status(
            exp_id, platform)
        message = MIMEMultipart()
        message['From'] = email.utils.formataddr(
            ('Autosubmit', self.config.MAIL_FROM))
        message['Subject'] = f'[Autosubmit] Warning: a remote platform is malfunctioning'
        message['Date'] = email.utils.formatdate(localtime=True)
        message.attach(MIMEText(message_text))

        files = []
        files_compressed = []
        try:
            files = [f for f in BasicConfig.expid_aslog_dir(
                exp_id).glob('*_run.log') if Path(f).is_file()]
            files.sort()
            files_compressed = [self._compress_file(f) for f in files[-1:]]
        except BaseException as e:
            Log.printlog(
                'An error has occurred while compressing log files for a warning email',
                6011)

        try:
            self._attach_files(message, files_compressed, files)
        except BaseException as e:
            Log.printlog(
                'An error has occurred while attaching log files to a warning email about remote_platforms',
                6011)

        for mail in mail_to:
            message['To'] = email.utils.formataddr((mail, mail))
            try:
                self._send_mail(self.config.MAIL_FROM, mail, message)
            except BaseException as e:
                Log.printlog(
                    'An error has occurred while sending a mail for warn about remote_platform',
                    6011)
        try:
            for f in files_compressed:
                Path.unlink(Path(f))
        except BaseException:
            Log.printlog(
                'An error has occurred while deleting compressed log files for a warning email',
                6011)

    def notify_status_change(
            self,
            exp_id,
            job_name,
            prev_status,
            status,
            mail_to):
        message_text = self._generate_message_text(
            exp_id, job_name, prev_status, status)
        message = MIMEText(message_text)
        message['From'] = email.utils.formataddr(
            ('Autosubmit', self.config.MAIL_FROM))
        message[
            'Subject'] = f'[Autosubmit] The job {job_name} status has changed to {str(status)}'
        message['Date'] = email.utils.formatdate(localtime=True)
        for mail in mail_to:  # expects a list
            message['To'] = email.utils.formataddr((mail, mail))
            try:
                self._send_mail(self.config.MAIL_FROM, mail, message)
            except BaseException as e:
                Log.printlog(
                    'Trace:{0}\nAn error has occurred while sending a mail for the job {0}'.format(
                        e, job_name), 6011)

    def _send_mail(self, mail_from, mail_to, message):
        server = smtplib.SMTP(self.config.SMTP_SERVER, timeout=60)
        server.sendmail(mail_from, mail_to, message.as_string())
        server.quit()

    def _attach_files(self, message, files, original_names):
        for i, f in enumerate(files) or []:
            with open(f, "rb") as file:
                part = MIMEApplication(file.read(), Name=Path(f).name)
                part['Content-Disposition'] = 'attachment; filename="%s.zip"' % Path(
                    original_names[i]).name
                message.attach(part)

    def _compress_file(self, file_path):
        temp_zip = tempfile.NamedTemporaryFile(
            delete=False, suffix='.zip', dir=Path(file_path).parent)
        zip_filename = temp_zip.name
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_path, Path(file_path).name)
        return zip_filename

    @staticmethod
    def _generate_message_text(exp_id, job_name, prev_status, status):
        return f'''Autosubmit notification\n
               -------------------------\n\n
               Experiment id:  {str(exp_id)} \n\n
               Job name: {str(job_name)} \n\n
               The job status has changed from: {str(prev_status)} to {str(status)} \n\n\n\n\n
               INFO: This message was auto generated by Autosubmit,
               remember that you can disable these messages on Autosubmit config file. \n'''

    @staticmethod
    def _generate_message_experiment_status(exp_id, platform=""):
        return f'''Autosubmit notification: Remote Platform issues\n-------------------------\n
               Experiment id: {str(exp_id)}
               Logs and errors: {BasicConfig.expid_aslog_dir(exp_id)}
               Attached to this message you will find the related _run.log files.

               Platform affected: {str(platform.name)} using as host: {str(platform.host)}\n\n[WARN] Autosubmit encountered an issue with a remote platform.\n It will resume itself, whenever is possible\n If this issue persists, you can change the host IP or put multiple hosts in the platform.yml file' + '\n\n\n\n\nINFO: This message was auto generated by Autosubmit,
                remember that you can disable these messages on Autosubmit config file.\n'''
