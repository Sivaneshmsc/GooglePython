# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/
import datetime
import logging

from googleapiclient import discovery
from google.cloud.bigquery import SchemaField
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials

PUBSUB_OAUTH_SCOPE = "https://www.googleapis.com/auth/pubsub"
BIGQUERY_OAUTH_SCOPE = "https://www.googleapis.com/auth/bigquery"
SERVICE_CREDS = ServiceAccountCredentials.from_json_keyfile_name(
    'keyfile.json', scopes=[PUBSUB_OAUTH_SCOPE, BIGQUERY_OAUTH_SCOPE])

_BQ_DTS_API_ENDPOINT = 'bigquerydatatransfer'
_BQ_DTS_API_VERSION = 'v1'

"""
startBigQueryJobs

* Creates ingestion-time table definitions
* Auto-retries load-job creation in case BigQuery is unavailable
* Sets TTL on table data (???)
* 
"""
def to_zulu_time(in_datetime):
    return in_datetime.isoformat('T') + 'Z'

def _dict_to_camel_case(in_dict):
    out_dict = dict()
    for key, value in in_dict.items():
        if value is None:
            continue

        out_key = _to_camel_case(key)
        out_dict[out_key] = value

    return out_dict


def _to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


class Enum(object):
    pass


class TransferState(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/TransferState
    TRANSFER_STATE_UNSPECIFIED = 'TRANSFER_STATE_UNSPECIFIED'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class MessageSeverity(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs.transferLogs#TransferMessage.MessageSeverity
    MESSAGE_SEVERITY_UNSPECIFIED = 'MESSAGE_SEVERITY_UNSPECIFIED'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'

class Format(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#Format
    FORMAT_UNSPECIFIED = 'FORMAT_UNSPECIFIED'
    CSV = 'CSV'
    JSON = 'JSON'
    AVRO = 'AVRO'
    PARQUET = 'PARQUET'
    RECORDIO = 'RECORDIO'
    CAPACITOR = 'CAPACITOR'
    COLUMNIO = 'COLUMNIO'


class Encoding(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#Encoding
    ENCODING_UNSPECIFIED = 'FORMAT_UNSPECIFIED'
    UTF8 = 'UTF8'
    ISO_8859_1 = 'ISO_8859_1'


class BQType(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#Type
    TYPE_UNSPECIFIED = 'TYPE_UNSPECIFIED'
    STRING = 'STRING'
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    RECORD = 'RECORD'
    BYTES = 'BYTES'
    BOOLEAN = 'BOOLEAN'
    TIMESTAMP = 'TIMESTAMP'
    DATE = 'DATE'
    TIME = 'TIME'
    DATETIME = 'DATETIME'
    NUMERIC = 'NUMERIC'


def TransferRun(name=None, state=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs#TransferRun

    # NOTE - Stripped down notion of TransferRun for patch()
    assert state in TransferState
    return _dict_to_camel_case(locals())


def TransferMessage(message_text=None, message_time=None, severity=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs.transferLogs#TransferMessage
    message_time = message_time or to_zulu_time(datetime.datetime.utcnow())
    severity = severity or MessageSeverity.INFO

    assert message_text
    assert severity in MessageSeverity
    return _dict_to_camel_case(locals())


def ImportedDataInfo(sql=None, destination_table_id=None, destination_table_description=None, table_defs=None, user_defined_functions=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#ImportedDataInfo
    assert destination_table_id
    assert type(table_defs) is list

    return _dict_to_camel_case(locals())


def TableDefinition(table_id=None, source_uris=None, format=None, max_bad_records=None, encoding=None, csv_options=None, schema=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#TableDefinition
    assert type(source_uris) is list
    return _dict_to_camel_case(locals())


def CsvOptions(field_delimiter=None, allow_quoted_newlines=None, quote_char=None, skip_leading_rows=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#CsvOptions
    return _dict_to_camel_case(locals())


def RecordSchema(fields=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#RecordSchema
    return _dict_to_camel_case(locals())


def FieldSchema(field_name=None, type=None, is_repeated=False, description='', schema=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#FieldSchema
    assert field_name

    # Assert (type == RECORD and SCHEMA) or (type != RECORD and not SCHEMA)
    assert not (bool(type == BQType.RECORD) ^ bool(schema))
    return _dict_to_camel_case(locals())


def FieldSchema_to_GCloudSchemaField(field_schema):
    bq_schema_field = dict()
    bq_schema_field['name'] = field_schema['fieldName']
    bq_schema_field['field_type'] = field_schema['type']
    bq_schema_field['description'] = field_schema['description']
    bq_schema_field['fields'] = RecordSchema_to_GCloudSchema(field_schema['schema'])
    return SchemaField(**bq_schema_field)


def RecordSchema_to_GCloudSchema(record_schema):
    """
    Converting from BigQuery Data Ingestion FieldSchema to BigQuery API FieldSchema

    TODO - Why the divergence?
    https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/

    :param field_schema:
    :return:
    """
    if not record_schema:
        return None

    return [FieldSchema_to_GCloudSchemaField(current_field) for current_field in record_schema['fields']]


##### BEGIN - Helper Ingestion Log Handler #####

 class TransferRunLogger(logging.Handler):
        LEVEL_TO_SEVERITY_MAP = {
            logging.INFO: MessageSeverity.INFO,
            logging.WARNING: MessageSeverity.WARNING,
            logging.ERROR: MessageSeverity.ERROR
        }

        def __init__(self, transfer_run_client=None, level=logging.NOTSET):
            super(TransferRunLogger, self).__init__(level=level)
            self._transfer_run_client = transfer_run_client
            self._msgs = list()

        def emit(self, record):
            # NOTE - Output record should be a dict or string depending
            msg_severity = self.LEVEL_TO_SEVERITY_MAP.get(record.level)
            if not msg_severity:
                return

            # Convert record to UTC time
            raw_datetime = datetime.datetime.fromtimestamp(record.created)
            msg_time = to_zulu_time(raw_datetime)
            msg_text = self.format(record)

            out_msg = TransferMessage(message_time=msg_time, severity=msg_severity, message_text=msg_text)

            self._msgs.append(out_msg)

        def flush(self):
            self._transfer_run_client.log_messages(transferMessages=self._msgs)
            self.reset()

        def reset(self):
            self._msgs = list()

##### END - Helper Ingestion Log Handler #####


class TransferRunPartnerClient(object):
    logger_cls = TransferRunLogger

    def __init__(self, credentials=None, project_id=None, location_id=None, config_id=None, run_id=None):
        # TODO - project_id = Customer's Project ID or Partner's Project ID?
        google_creds = credentials or GoogleCredentials.get_application_default()
        scoped_creds = google_creds.create_scoped(OAUTH_SCOPE)

        self._api_client = discovery.build(
            _BQ_DTS_API_ENDPOINT, _BQ_DTS_API_VERSION,
            credentials=scoped_creds)

        self.project_id = project_id
        self.location_id = location_id
        self.config_id = config_id
        self.run_id = run_id

        log_handler = self.logger_cls(transfer_run_client=self)
        self.logger = logging.getLogger(self.__class__.__module__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(log_handler)

    def _execute_api_call(self, method_name, body=None, **kwargs):
        """
        Convenience method

        > return self._api_call('finishRun')

        Equivalent to

        > return self._api_client.projects().locations().transferConfigs().runs().finishRun(
            projectId=self.project_id,
            locationId=self.location_id,
            config_id=self.config_id,
            run_id=self.run_id
        ).execute()
        """

        api_prefix_fxn = self._api_client.projects().locations().transferConfigs().runs()
        api_fxn = getattr(api_prefix_fxn, method_name)

        api_params = kwargs.deepcopy()
        api_params['projectId'] = self.project_id
        api_params['locationId'] = self.location_id
        api_params['config_id'] = self.config_id
        api_params['run_id'] = self.run_id
        if body is not None:
            api_params['body'] = body

        built_api_callable = api_fxn(**api_params)
        return built_api_callable.execute()

    def finish_run(self):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/finishRun
        return self._execute_api_call('finishRun')

    def log_messages(self, body):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/logMessages
        return self._execute_api_call('logMessages', body=body)

    def patch(self, body, update_mask=''):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/patch
        return self._execute_api_call('patch', body=body, updateMask=update_mask)

    def start_big_query_jobs(self, body):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs
        return self._execute_api_call('startBigQueryJobs', body=body)
