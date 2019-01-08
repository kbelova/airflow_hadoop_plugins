from hooks.impala_hook import ImpalaHook
from airflow.models import BaseOperator
from airflow import configuration
from airflow.utils.decorators import apply_defaults

from airflow.exceptions import AirflowException

import logging


class ImpalaQueryOperator(BaseOperator):
    def execute(self, context):
        logging.debug(f'Executing: {self.sql}')
        logging.debug(f'Context is: {str(context)}')
        self.hook = self.get_hook()

        # set the mapred_job_name if it's not set with dag, task, execution time info
        if not self.mapred_job_name:
            ti = context['ti']
            logging.debug(f'Context is: {str(ti)}')
            logging.debug(f'Hostname is: {str(ti.hostname)}')
            self.hook.mapred_job_name = self.mapred_job_name_template \
                .format(ti.dag_id, ti.task_id,
                        ti.execution_date.isoformat(),
                        ti.hostname.split('.')[0])

        logging.debug(f"Run sql: {self.sql}")
        self.hook.run(sql=self.sql, schema=self.schema)

    template_fields = ('sql', 'schema', 'impala_conn_id', 'mapred_queue',
                       'mapred_job_name', 'mapred_queue_priority')
    template_ext = ('.hql', '.sql',)
    ui_color = '#000080'  # Apache Impala Main Color: Navy

    @apply_defaults
    def __init__(
            self, sql,
            impala_conn_id='impala',
            schema='default',
            script_begin_tag=None,
            run_as_owner=False,
            mapred_queue=None,
            mapred_queue_priority=None,
            mapred_job_name=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.impala_conn_id = impala_conn_id
        self.schema = schema
        self.script_begin_tag = script_begin_tag
        self.run_as = None
        if run_as_owner:
            self.run_as = self.dag.owner
        self.mapred_queue = mapred_queue
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name
        self.mapred_job_name_template = configuration.get('impala',
                                                          'mapred_job_name_template')
        self.hook = None

    def get_hook(self):
        return ImpalaHook(conn_id=self.impala_conn_id)
