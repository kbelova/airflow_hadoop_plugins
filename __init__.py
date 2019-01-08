from airflow.plugins_manager import AirflowPlugin

from hooks.impala_hook import ImpalaHook


# Defining the plugin class
class ImpalaOperatorPlugin(AirflowPlugin):
    name = "impala_operator_plugin"
    operators = [ImpalaQueryOperator]
    flask_blueprints = []
    hooks = [ImpalaHook]
    executors = []
    admin_views = []
    menu_links = []
