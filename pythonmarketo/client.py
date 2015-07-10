import time
from pythonmarketo.helper.exceptions import MarketoException
from pythonmarketo.helper.http_lib import HttpLib


class MarketoClient(object):

    host = None
    client_id = None
    client_secret = None
    token = None
    expires_in = None
    valid_until = None
    token_type = None
    scope = None
    last_request_id = None
    API_CALLS_MADE = 0
    API_LIMIT = None

    def __init__(self, host, client_id, client_secret, api_limit=None):
        assert(host is not None)
        assert(client_id is not None)
        assert(client_secret is not None)
        self.host = host
        self.client_id = client_id
        self.client_secret = client_secret
        self.API_LIMIT = api_limit

    def execute(self, method, *args, **kargs):
        result = None
        if self.API_LIMIT and self.API_CALLS_MADE >= self.API_LIMIT:
            raise Exception(
                {
                    'message': 'API Calls exceded the limit: {}'.format(str(self.API_LIMIT)),
                    'code': '416'
                }
            )

        # max 10 rechecks
        for i in range(0, 10):
            try:
                method_map = {
                    'get_leads': self.get_leads,
                    'get_leads_by_listId': self.get_leads_by_listId,
                    'get_activity_types': self.get_activity_types,
                    'get_lead_activity': self.get_lead_activity,
                    'get_paging_token': self.get_paging_token,
                    'update_lead': self.update_lead,
                    'create_lead': self.create_lead,
                    'create_or_update_lead': self.create_or_update_lead,
                    'create_or_update_and_associate_lead': self.create_or_update_and_associate_lead,
                    'get_lead_activity_page': self.get_lead_activity_page,
                    'get_email_content_by_id': self.get_email_content_by_id,
                    'get_email_template_content_by_id': self.get_email_template_content_by_id,
                    'get_email_templates': self.get_email_templates,
                }

                result = method_map[method](*args, **kargs)
                self.API_CALLS_MADE += 1
            except MarketoException as e:
                '''
                601 -> auth token not valid
                602 -> auth token expired
                '''
                if e.code in ['601', '602']:
                    continue
                else:
                    raise Exception({'message': e.message, 'code': e.code})
            break
        return result

    def authenticate(self):
        if self.valid_until is not None and self.valid_until > time.time():
            return
        args = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        data = HttpLib().get("https://{host}/identity/oauth/token".format(host=self.host), args)
        if data is None:
            raise Exception("Empty Response")
        self.token = data['access_token']
        self.token_type = data['token_type']
        self.expires_in = data['expires_in']
        self.valid_until = time.time() + data['expires_in']
        self.scope = data['scope']
        self.args = {
            'access_token': self.token
        }

    def get_leads(self, filtr, values=[], fields=[]):
        self.authenticate()
        values = values.split() if type(values) is str else values
        args = {
            'access_token': self.token,
            'filterType': str(filtr),
            'filterValues': (',').join(values)
        }
        if len(fields) > 0:
            args['fields'] = ",".join(fields)
        data = HttpLib().get("https://{host}/rest/v1/leads.json".format(host=self.host), args)
        if data is None:
            raise Exception("Empty Response")
        self.last_request_id = data['requestId']
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data['result']

    def get_email_templates(self, offset, maxreturn, status=None):
        self.authenticate()
        if id is None:
            raise ValueError("Invalid argument:required argument id is none.")
        args = {
            'access_token': self.token,
            'offset': offset,
            'maxreturn': maxreturn
        }
        if status is not None:
            args['status'] = status
        data = HttpLib().get("https://{host}/rest/asset/v1/emailTemplates.json".format(host=self.host), args)
        if data is None:
            raise Exception("Empty Response")
        self.last_request_id = data['requestId']
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data['result']

    def get_email_content_by_id(self, id):
        self.authenticate()
        if id is None:
            raise ValueError("Invalid argument: required argument id is none.")
        args = {
            'access_token': self.token
        }
        data = HttpLib().get(
            "https://{host}/rest/asset/v1/email/{id}/content".format(
                host=self.host,
                id=str(id)
            ), args
        )
        if data is None:
            raise Exception("Empty Response")
        self.last_request_id = data['requestId']
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data['result']

    def get_email_template_content_by_id(self, id, status=None):
        self.authenticate()
        if id is None:
            raise ValueError("Invalid argument:required argument id is none.")
        args = {
            'access_token': self.token
        }
        if status is not None:
            args['status'] = status
        data = HttpLib().get(
            "https://{host}/rest/asset/v1/emailTemplate/{id}/content".format(
                host=self.host,
                id=str(id)
            ),
            args
        )
        if data is None:
            raise Exception("Empty Response")
        self.last_request_id = data['requestId']
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data['result']

    def get_leads_by_listId(self, listId=None, batchSize=None, fields=[]):
        self.authenticate()
        args = {
            'access_token': self.token
        }
        if len(fields) > 0:
            args['fields'] = ",".join(fields)
        if batchSize:
            args['batchSize'] = batchSize
        result_list = []
        while True:
            data = HttpLib().get(
                "https://{host}/rest/v1/list/{listId}/leads.json".format(
                    host=self.host,
                    listId=str(listId)
                ),
                args
            )
            if data is None:
                raise Exception("Empty Response")
            self.last_request_id = data['requestId']
            if not data['success']:
                raise MarketoException(data['errors'][0])
            result_list.extend(data['result'])
            if len(data['result']) == 0 or 'nextPageToken' not in data:
                break
            args['nextPageToken'] = data['nextPageToken']
        return result_list

    def get_activity_types(self):
        self.authenticate()
        args = {
            'access_token': self.token
        }
        data = HttpLib().get("https://{host}/rest/v1/activities/types.json".format(host=self.host), args)
        if data is None:
            raise Exception("Empty Response")
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data['result']

    def get_lead_activity_page(self, activityTypeIds, nextPageToken, batchSize=None, listId=None):
        self.authenticate()
        activityTypeIds = activityTypeIds.split() if type(activityTypeIds) is str else activityTypeIds
        args = {
            'access_token': self.token,
            'activityTypeIds': ",".join(activityTypeIds),
            'nextPageToken': nextPageToken
        }
        if listId:
            args['listId'] = listId
        if batchSize:
            args['batchSize'] = batchSize
        data = HttpLib().get("https://{host}/rest/v1/activities.json".format(host=self.host), args)
        if data is None:
            raise Exception("Empty Response")
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data

    def get_lead_activity(self, activityTypeIds, sinceDatetime, batchSize=None, listId=None):
        activity_result_list = []
        nextPageToken = self.get_paging_token(sinceDatetime=sinceDatetime)
        moreResult = True
        while moreResult:
            result = self.get_lead_activity_page(activityTypeIds, nextPageToken, batchSize, listId)
            if result is None:
                break
            moreResult = result['moreResult']
            nextPageToken = result['nextPageToken']
            if 'result' in result:
                activity_result_list.extend(result['result'])

        return activity_result_list

    def get_paging_token(self, sinceDatetime):
        self.authenticate()
        args = {
            'access_token': self.token,
            'sinceDatetime': sinceDatetime
        }
        data = HttpLib().get("https://{host}/rest/v1/activities/pagingtoken.json".format(host=self.host), args)
        if data is None:
            raise Exception("Empty Response")
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data['nextPageToken']

    def update_lead(self, lookupField, lookupValue, values):
        updated_lead = dict(list({lookupField: lookupValue}.items()) + list(values.items()))
        data = {
            'action': 'updateOnly',
            'lookupField': lookupField,
            'input': [updated_lead]
        }
        return self.post(data)

    def create_lead(self, lookupField, lookupValue, values):
        new_lead = dict(list({lookupField: lookupValue}.items()) + list(values.items()))
        data = {
            'action': 'createOnly',
            'lookupField': lookupField,
            'input': [new_lead]
        }
        return self.post(data)

    def create_or_update_lead(self, lookupField, lookupValue, values):
        url = "https://{host}/rest/v1/leads.json".format(host=self.host)
        lead = dict(list({lookupField: lookupValue}.items()) + list(values.items()))
        data = {
            'action': 'createOrUpdate',
            'lookupField': lookupField,
            'input': [lead]
        }
        return self._post(url, data)

    def create_or_update_and_associate_lead(self, lookupField, lookupValue, cookie, values, **kwargs):
        lead = self.create_or_update_lead(lookupField, lookupValue, values)
        lead_id = lead['result'][0]['id']
        url = "https://{host}/rest/v2/leads/{lead_id}/associate.json".format(
            host=self.host,
            lead_id=lead_id
        )
        data = {'cookie': cookie}
        return self._post(url, data)

    def _post(self, url, args=None, data=None):
        self.authenticate()
        if args:
            args = self.args.update(args)
        else:
            args = self.args

        response = HttpLib().post(url, args, data)
        if not response['success']:
            raise MarketoException(data['errors'][0])
        return response

    def post(self, data):
        self.authenticate()
        args = {
            'access_token': self.token
        }
        data = HttpLib().post("https://{host}/rest/v1/leads.json".format(host=self.host), args, data)
        if not data['success']:
            raise MarketoException(data['errors'][0])
        return data['result'][0]

