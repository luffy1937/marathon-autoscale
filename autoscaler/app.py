import requests
import sys
import logging
import threading

class MarathonApp:

    MARATHON_APPS_URI = '/service/marathon/v2/apps'

    def __init__(self, app_id, api_client, dcos_tenant):
        '''
        支持多租户
        :param app_id:
        :param api_client:
        :param dcos_tenant: 租户
        '''
        self.app_id = app_id
        self.api_client = api_client
        self.log = logging.getLogger('autoscale')
        self.marathon_apps_uri = MarathonApp.MARATHON_APPS_URI.replace('marathon', dcos_tenant)
        #app_name不同于app_id
        self.app_name = None
    def app_exists(self):
        """Determines if the application exists in Marathon
        """
        try:
            response = self.api_client.dcos_rest(
                "get",
                self.marathon_apps_uri + self.app_id
            )
            if(None != response['app'].get('env') and None != response['app'].get('env').get('APP_NAME')):
                self.app_name = response['app'].get('env').get('APP_NAME')
            return self.app_id == response['app']['id']
        except requests.exceptions.HTTPError as e:
            if e.response is not None:
                if e.response.status_code != 404:
                    raise

        return False

    def get_app_instances(self):
        """Returns the number of running tasks for a given Marathon app"""
        app_instances = 0

        response = self.api_client.dcos_rest(
            "get",
            self.marathon_apps_uri + self.app_id
        )

        try:
            app_instances = response['app']['instances']
            self.log.debug("Marathon app %s has %s deployed instances",
                           self.app_id, app_instances)
        except KeyError:
            self.log.error('No task data in marathon for app %s', self.app_id)


        return app_instances

    def get_app_details(self):
        """Retrieve metadata about marathon_app
        Returns:
            Dictionary of task_id mapped to mesos slave_id
        """
        app_task_dict = {}

        response = self.api_client.dcos_rest(
            "get",
            self.marathon_apps_uri + self.app_id
        )

        try:
            for i in response['app']['tasks']:
                taskid = i['id']
                hostid = i['host']
                slave_id = i['slaveId']
                self.log.debug("Task %s is running on host %s with slaveId %s",
                               taskid, hostid, slave_id)
                app_task_dict[str(taskid)] = str(slave_id)
        except KeyError:
            self.log.error('No task data in marathon for app %s', self.app_id)

        return app_task_dict
