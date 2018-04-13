# Copyright 2018 Google LLC All Rights Reserved.
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

import argparse
import datetime
import json
import path
import re
import sys
import time
import StringIO

from bq_dts import api_client

from google.cloud import bigquery
from google.cloud import pubsub
from google.cloud import storage
from google.cloud import exceptions
from google.cloud.bigquery import LoadJobConfig

def _regex_parser(parser_regex, fields):
    def _parse(current_str):
        matcher = re.match(parser_regex, current_str)
        groups = matcher.groupdict()

        output_tuple = tuple(groups[field_name] for field_name in fields)
        return output_tuple

    return _parse

TRANSFER_RUN_NAME_PARSER = re.compile('projects/(?P<project_id>.*?)/locations/(?P<location_id>.*?)/transferConfigs/(?P<config_id>.*?)/runs/(?P<run_id>.*?)')
parse_transfer_run_name = _regex_parser(TRANSFER_RUN_NAME_PARSER, ['project_id', 'location_id', 'config_id', 'run_id'])

##### BEGIN - GCS Helpers #####

GCS_URI_PARSER = re.compile('gs://(?P<bucket>.*?)/(?P<blob>.*?)$')
parse_gcs_uri = _regex_parser(GCS_URI_PARSER, ['bucket', 'blob'])


def upload_uri_to_gcs(gcs_client, src_filename, gcs_uri, overwrite=False):
    gcs_bucket, gcs_blob = parse_gcs_uri(gcs_uri)
    bucket_obj = gcs_client.get_bucket(gcs_bucket)

    blob_obj = bucket_obj.blob(gcs_blob)
    if blob_obj.exists() and overwrite:
        return

    blob_obj.upload_from_filename(filename=src_filename)

##### END - GCS Helpers #####


BQ_DTS_FORMAT_TO_BQ_SOURCE_FORMAT_MAP = {
    api_client.Format.FORMAT_UNSPECIFIED: 'CSV',
    api_client.Format.CSV: 'CSV',
    api_client.Format.JSON: 'NEWLINE_DELIMITED_JSON',
    api_client.Format.AVRO: 'AVRO',
    api_client.Format.RECORDIO: None,
    api_client.Format.CAPACITOR: None,
    api_client.Format.COLUMNIO: None,
}

def date_from_timestamp(in_timestamp):
    return datetime.datetime.fromtimestamp(in_timestamp)


def _raw_field_to_fieldschema(raw_field):
    out_field = dict()
    out_field['field_name'] = raw_field['name']
    out_field['type'] = field_type = raw_field['type']
    out_field['is_repeated'] = raw_field.get('is_repeated', False)
    out_field['description'] = raw_field.get('desc', '')

    if field_type == api_client.BQType.RECORD:
        out_field['schema'] = _raw_fields_to_record(raw_field['fields'])

    return api_client.FieldSchema(**out_field)


def _raw_fields_to_record(raw_fields):
    fields = [
        _raw_field_to_fieldschema(current_field) for current_field in raw_fields
    ]
    return api_client.RecordSchema(fields=fields)


class BaseSource(object):
    # Table prefix with date_suffix
    # TODO - String together BQ DTS logger and local logger
    # TODO - Dynamically determine local scratch dir based on run-name
    # TODO - Dynamically determine scratch GCS bucket based on location + source

    TABLES = list()
    _TABLES_MAP = dict()

    pubsub_payload_format = 'json'

    ##### BEGIN - Methods to script init options #####
    def __init__(self, credentials=None):
        # Setup GCP Clients
        self._ps_client = None
        self._gcs_client = None
        self._bq_client = None

        # Setup pre-built RecordSchemas
        for table_info in self.TABLES:
            built_table = dict()
            built_table['name'] = table_name = table_info['name']
            built_table['desc'] = table_info.get('desc', '')
            built_table['schema'] = _raw_fields_to_record(built_table['fields'])
            built_table['pattern'] = table_info['pattern']
            self._TABLES_MAP[table_name] = built_table

        self._credentials = credentials

    def setup_args(self):
        self._parser = argparse.ArgumentParser()
        self._parser.add_argument('--subscription', dest='subscription')
        self._parser.add_argument('--gcs-prefix', dest='gcs_prefix', type=path.path, required=True)
        self._parser.add_argument('--gcs-overwrite', dest='gcs_overwrite', action='store_true', default=False)
        self._parser.add_argument('--local-prefix', dest='local_prefix', type=path.path, default='/tmp/')

        self._parser.add_argument('--use-bq-load', dest='use_bq_load', action='store_true', default=False)

    def process_args(self, args=None):
        self._opts = self._parser.parse_args(args=args)
    ##### END - Methods to script init options #####


    ##### BEGIN - Methods to initiate TransferRun processing #####
    def run(self, args=None):
        self.setup_args()
        self.process_args(args=args)

        # Process runs from PubSub when given a subscription
        # Otherwise, read an IngestionRun JSON from stdin
        if self._opts.subscription:
            self.listen_for_runs_pubsub(self._opts.subscription)
        else:
            self.listen_for_run_stdin()

    def listen_for_runs_pubsub(self, subscription_name):
        # Setup the pubsub Subscription and the associated pubsub client
        subscription = pubsub.Subscription(subscription_name, client=self.ps_client)
        while True:
            try:
                pull_resp = subscription.pull(max_messages=1)
            except exceptions.GoogleCloudError:
                continue

            ack_ids = []
            for ack_id, rcv_msg in pull_resp:
                # TODO - Assumes transferPubSubPayloadFormat == JSON
                current_run = json.loads(rcv_msg.data)

                self.process_run(current_run)

                ack_ids.append(ack_id)

            subscription.acknowledge(ack_ids=ack_ids)

    def listen_for_run_stdin(self):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs#TransferRun
        string_buffer = StringIO.StringIO()
        for line in sys.stdin:
            string_buffer.write(line)

        current_run = _transfer_run_from_json(string_buffer.getvalue())

        self.process_run(current_run)

    def process_run(self, current_run):
        if self._opts.use_bq_load:
            return self.process_run_with_bq_load_jobs(current_run)

        return self.process_run_with_bq_dts(current_run)
    ##### END - Methods to initiate TransferRun processing #####


    ##### BEGIN - Methods to stage requested data #####
    def stage_data_for_transfer_run(self, current_run):
        """
        Step 1) Stage local tables
        Step 2) Upload local tables to GCS
        Step 3) Setup ImportedDataInfo's for use with startBigQueryJobs

        :param current_date:
        :param current_run:
        https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs#resource-transferrun
        :return:
        """
        current_date = date_from_timestamp(current_run['run_time']['seconds'])

        # Step 1) Stage local tables
        table_to_local_uris_map = self.stage_tables_locally(current_date, current_run)

        # Step 2) Upload local tables to GCS
        table_to_gcs_uris_map = self.stage_tables_in_gcs(table_to_local_uris_map)

        # Step 3) Setup ImportedDataInfo for use with startBigQueryJobs
        run_idis = []
        for current_table, current_gcs_uris in table_to_gcs_uris_map.items():
            # Step 3a) Create target table name
            output_table = current_table.format(date=current_date, params=current_run['params'])

            # Step 3b) Fetch target table description
            output_table_desc = self._TABLES_MAP[current_table].get('desc')

            # Step 3c) Pull pre-built schemas by un-templated table name
            output_schema = self._TABLES_MAP[current_table]['schema']

            # Step 3d) Pull down the RecordSchema associated with this raw table name
            table_as_idi = self.convert_table_map_to_idi(output_table, output_table_desc, output_schema, current_gcs_uris)
            run_idis.append(table_as_idi)

        return run_idis

    def stage_tables_locally(self, current_date, current_run):
        """
        :param current_date:
        :param current_run:
        :return: table_to_src_uris_map
        """
        raise NotImplementedError

    def stage_tables_in_gcs(self, table_to_local_uris_map):
        table_to_gcs_uris_map = dict()

        for table, local_uris in table_to_local_uris_map.items():
            output_gcs_uris = []
            for current_uri in local_uris:
                # Convert local file path to GCS file path
                gcs_uri = current_uri.replace(self._opts.local_prefix, self._opts.gcs_prefix)

                upload_uri_to_gcs(self.gcs_client, current_uri, gcs_uri, overwrite=self._opts.gcs_overwrite)

                output_gcs_uris.append(gcs_uri)

            table_to_gcs_uris_map[table] = output_gcs_uris

        return table_to_gcs_uris_map

    @classmethod
    def convert_table_map_to_idi(cls, tgt_table, tgt_table_desc, schema, src_uris):
        tbl_def = dict()
        tbl_def['source_uris'] = src_uris
        tbl_def['format'] = api_client.Format.JSON
        tbl_def['max_bad_records'] = 0
        tbl_def['encoding'] = api_client.Encoding.UTF8
        tbl_def['schema'] = schema

        # Create ImportedDataInfo, one for each table to be loaded
        tbl_def_obj = api_client.TableDefinition(**tbl_def)

        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#ImportedDataInfo
        idi = dict()
        idi['destination_table_id'] = tgt_table
        idi['destination_table_description'] = tgt_table_desc
        idi['table_defs'] = [tbl_def_obj]

        idi_obj = api_client.ImportedDataInfo(**idi)
        return idi_obj
    ##### END - Methods to stage requested data #####


    ##### BEGIN - Methods for BQ DTS-managed loads #####
    def process_run_with_bq_dts(self, current_run):
        # Step 1 - Setup a TransferRun-specific client
        project_id, location_id, config_id, run_id = parse_transfer_run_name(current_run['name'])
        transfer_run_client = api_client.TransferRunPartnerClient(
                credentials=self._credentials,
                project_id=project_id, location_id=location_id, config_id=config_id, run_id=run_id)

        # Step 2 - Let BQ DTS we're working
        transfer_run_client.patch(body=dict(state=api_client.TransferState.RUNNING), update_mask='state')

        # Step 3 - Stage data for your transfer run
        run_idis = self.stage_data_for_transfer_run(current_run)

        # Step 4 - Trigger a single startBigQueryJobs call
        body = dict()
        body['importedData'] = run_idis
        body['userCredentials'] = None # TODO - Is this required?
        transfer_run_client.start_big_query_jobs(body=body)

        # Step 5 - Notify BQ DTS your work is done and let BQ DTS manage the BigQuery loads
        transfer_run_client.finish_run()
    ##### END - Methods for BQ DTS-managed loads #####

    ##### BEGIN - Methods for self-managed loads #####
    def process_run_with_bq_load_jobs(self, current_run):
        """
        Useful for debugging purposes
        """
        # Step 1 - Stage data for your transfer run
        run_idis = self.stage_data_for_transfer_run(current_run)

        # Step 2 - Ensure the target Dataset exists
        dataset_id = current_run['destinationDatasetId']
        dataset_ref = self.bq_client.dataset(dataset_id)

        # Step 3 - Trigger multiple BigQuery Load jobs based on the ImportedDataInfo
        for current_idi in run_idis:
            self.load_table_via_bq_load_job(dataset_ref, current_idi)

    def load_table_via_bq_load_job(self, dataset_ref, current_idi):
        """
        Assumes staged data is
        :param dataset_ref:
        :param current_idi:
        :return:
        """
        # Step 1 - Translate required fields for BigQuery Python SDK
        tgt_table_name = current_idi['destinationTableId']
        tgt_tabledef = current_idi['tableDefs'][0]
        tgt_schema = api_client.RecordSchema_to_GCloudSchema(tgt_tabledef['schema'])

        # Step 2 - Create target table if it doesn't exist
        table_ref = dataset_ref.table(tgt_table_name)
        tgt_table = bigquery.Table(table_ref, schema=tgt_schema)
        if not tgt_table.exists():
            tgt_table = self.bq_client.create_table(tgt_table)

        # Step 3 - Create a BigQuery Load Job based on the URIs
        src_uris = tgt_tabledef['sourceUris']
        job_name = '{}_{:d}'.format(tgt_table_name, int(time.time()))

        # https://googlecloudplatform.github.io/google-cloud-python/latest/_modules/google/cloud/bigquery/client.html#Client.load_table_from_uri
        # https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/reference.html#google.cloud.bigquery.job.LoadJob
        job_config = LoadJobConfig()

        # NOTE - Source Format from BQ DTS does NOT match BQ SourceFormat, why did we re-create these?
        # BQ DTS -  https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs#Format
        # BQ Load - https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.sourceFormat
        source_format = BQ_DTS_FORMAT_TO_BQ_SOURCE_FORMAT_MAP[tgt_tabledef['format']]
        assert source_format is not None
        job_config.source_format = source_format
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        load_job = self.bq_client.load_table_from_uri(source_uris=src_uris, destination=tgt_table,
            job_id=job_name, job_config=job_config)

        return load_job

    ##### END - Methods  for self-managed loads #####

    @property
    def ps_client(self):
        if not self._ps_client:
            self._ps_client = pubsub.Client()
        return self._ps_client

    @property
    def gcs_client(self):
        if not self._gcs_client:
            self._gcs_client = storage.Client()
        return self._gcs_client

    @property
    def bq_client(self):
        if not self._bq_client:
            self._bq_client = bigquery.Client()
        return self._bq_client
