#!/usr/bin/env python3

import argparse
import json
import os
import re
import requests
import sys
import traceback

from datetime import datetime, date, timedelta
from jsondiff import diff
from pprint import pformat
from settings import SENDGRID_API_KEY
from sendgrid.helpers.mail import *
from sendgrid import *

# Requirements: sudo pip install jsondiff

# This script assumes that it is placed in the '/home/projects/openspending-check'
# directory and also run from this directory. Add the following line to
# 'sudo crontab -e' to run it every day:
#0 0 * * * (cd /home/projects/openspending-check && sudo ./check-openspending.py)

class OpenSpendingChecker:
    def __init__(self):
        parser = argparse.ArgumentParser(description=
            'Checks for new CBS Iv3 updates every day and emails us if new Iv3 data'
            'is found'
        )

        parser.add_argument('-t', '--today', action="store", dest="today_date")
        parser.add_argument('-y', '--yesterday', action="store", dest="yesterday_date")
        self.args = parser.parse_args()
        self.log_dir = 'daily-results'

        self.today_date = date.today()
        if self.args.today_date:
            self.today_date = self.args.today_date
        self.today_file = f"{self.log_dir}/{self.today_date}.json"

        yesterday_date = date.today() - timedelta(days=1)
        if self.args.yesterday_date:
            yesterday_date = self.args.yesterday_date
        self.yesterday_file = f"{self.log_dir}/{yesterday_date}.json"

    @classmethod
    def sendmail(cls, subject, content, to=['developers@openstate.eu']):
        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
        mail = Mail()
        email = Email()
        email.email = "developers@openstate.eu"
        email.name = "Open State Foundation developers"
        mail.from_email = email
        mail.subject = subject

        personalization = Personalization()
        for address in to:
            personalization.add_to(Email(address))
        mail.add_personalization(personalization)

        mail.add_content(Content("text/plain", content))

        sg.client.mail.send.post(request_body=mail.get())

    def get_new_data(self):
        cbs_iv3_catalog = 'https://dataderden.cbs.nl/ODatacatalog/Tables?$format=json'

        s = requests.session()

        result = s.get(cbs_iv3_catalog).json()

        # Save only the items related to Iv3 data
        all_items = {}
        for item in result['value']:
            identifier = item['Identifier']
            if item['Catalog'] == 'IV3':
                del item['ID']
                all_items[identifier] = item

        # Don't write to the today_file if we passed the today_date as an
        # argument as this means that the file already exists
        if not self.args.today_date:
            with open(self.today_file, 'w') as OUT:
                json.dump(all_items, OUT, indent=4, sort_keys=True)

        try:
            with open(self.today_file) as IN:
                return [True, json.load(IN)]
        except:
            self.sendmail(
                '[Open Spending Checker] Could not find today\'s test',
                'The automated daily check for Open Spending updates could not find today\'s test file \'%s\'' % (self.today_file)
            )
            return [False, None]

    def get_old_data(self):
        try:
            with open(self.yesterday_file) as IN:
                return [True, json.load(IN)]
        except:
            self.sendmail(
                '[Open Spending Checker] Could not find yesterday\'s test',
                'The automated daily check for Open Spending updates could not find yesterday\'s test file \'%s\'' % (self.yesterday_file)
            )
            return [False, None]

    def process(self):
        result, today = self.get_new_data()
        if not result: return
        if not today:
            self.sendmail(
                '[Open Spending Checker] Today\'s test is empty',
                'The automated daily check for Open Spending updates could not find results in today\'s test file \'%s\'' % (self.today_file)
            )
            return

        result, yesterday = self.get_old_data()
        if not result: return
        if not yesterday:
            self.sendmail( 
                '[Open Spending Checker] Yesterday\'s test is empty',
                'The automated daily check for Open Spending updates could not find results in yesterday\'s test file \'%s\'' % (self.yesterday_file)
            )
            return


        # Format of changes:
        #   - keys are the dataset names, e.g. '45005NED', '45006NED', '45007NED'
        #   - if an insert or delete has taken place, the key is an object so e.g. {insert: ['a']}
        #   - pass 'marshal="True"' to diff to turn those keys into strings prepended by a '$', so the above would become {'$insert': ['a']}
        changes = diff(yesterday, today, syntax='symmetric')
        if changes:
            self.sendmail(
                '[Open Spending Checker] Found changes in CBS data, time to update!',
                'The automated daily check for Open Spending updates detected the following changes in the test results compared to yesterday:\n\n%s' % (pformat(changes, indent=2))
            )

try:
    checker = OpenSpendingChecker()
    checker.process()
except Exception as e:
    OpenSpendingChecker.sendmail(
        '[Open Spending Checker] Exception occurred',
        f'{str(e)} {traceback.print_tb(e.__traceback__)}'
    )


# Code for old Open Spending
## New datasets (i.e., year) are added using '$insert' in changes so
## detect that and restructure the dict accordingly by moving the
## new datasets one level up in the dict
#for k, v in changes.items():
#    if str(k) == '$insert':
#        for new_key in v.keys():
#            changes[new_key] = v[new_key]
#        del changes[k]
#
## Remove changes which did not modify the data (this happens in rare
## occasions, e.g. when a field is added to this table and we thus
## don't have to update the data
#for k, v in changes.items():
#    if not 'Modified' in v:
#        del changes[k]
#
#if changes:
#    timestamp = datetime.now().isoformat()
#
#    for table in changes:
#        # Try to download the tables that contain changes
#        download_cbs_data_log = 'log/download_cbs_data.log'
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && echo -e \"\n\n\n%s\n\n\" >> %s 2>&1'" % (timestamp, download_cbs_data_log)
#        os.system(cmd)
#
#        r = requests.get('https://dataderden.cbs.nl/ODataAPI/OData/%s/' % table).json()
#        endpoints = [x['name'] for x in r['value']]
#        endpoints.remove('TypedDataSet')
#        endpoints.remove('DataProperties')
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && ./manage.py download_cbs_data --traceback -i %s" % (table)
#        for endpoint in endpoints:
#            cmd += " -t %s" % (endpoint)
#        cmd += " >> %s 2>&1'" % (download_cbs_data_log)
#        if os.system(cmd) != 0:
#            mail_error(cmd)
#            continue
#
#        # Try to run cbs2document
#        cbs2document_log = 'log/cbs2document.log'
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && echo -e \"\n\n\n%s\n\n\" >> %s 2>&1'" % (timestamp, cbs2document_log)
#        os.system(cmd)
#
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && ./manage.py cbs2document --traceback -i %s -t UntypedDataSet_orig >> %s 2>&1'" % (table, cbs2document_log)
#        if os.system(cmd) != 0:
#            mail_error(cmd)
#            continue
#
#        # Try to run save_government_model
#        save_government_model_log = 'log/save_government_model.log'
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && echo -e \"\n\n\n%s\n\n\" >> %s 2>&1'" % (timestamp, save_government_model_log)
#        os.system(cmd)
#
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && ./manage.py save_government_model --traceback -i %s >> %s 2>&1'" % (table, save_government_model_log)
#        if os.system(cmd) != 0:
#            mail_error(cmd)
#            continue
#
#        # Try to run import_cbs_data
#        import_cbs_data_log = 'log/import_cbs_data.log'
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && echo -e \"\n\n\n%s\n\n\" >> %s 2>&1'" % (timestamp, import_cbs_data_log )
#        os.system(cmd)
#
#        cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && ./manage.py import_cbs_data --traceback -i %s -t UntypedDataSet -p 6 >> %s 2>&1'" % (table, import_cbs_data_log )
#        if os.system(cmd) != 0:
#            mail_error(cmd)
#            continue
#
#        # If we reach this point then all updates should have been
#        # successful
#        sendmail(
#            '[Open Spending Checker] Successfully updated %s' % (table),
#            'Updates have been successfully processed for %s' % (table)
#        )
#
#    # Try to run download_metrics
#    download_metrics_log = 'log/download_metrics.log'
#    cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && echo -e \"\n\n\n%s\n\n\" >> %s 2>&1'" % (timestamp, download_metrics_log)
#    os.system(cmd)
#
#    cmd = "sudo docker exec c-openspending bash -c 'source ../bin/activate && ./manage.py download_metrics --traceback -a >> %s 2>&1'" % (download_metrics_log)
#    if os.system(cmd) != 0:
#        mail_error(cmd)
