from .processor import TapProcessor
from ..Helpers import get_selected_streams


class LoanAccountsProcessor(TapProcessor):
    def _process_child_records(self, record):
        from .. import sync_endpoint_refactor

        total_records = super()._process_child_records(record)
        children_configs = self.generators[0].endpoint_config.get('children', {})
        for child_config_key in children_configs:
            if child_config_key in get_selected_streams(self.catalog):
                child_config = children_configs[child_config_key]
                child_config = dict(child_config)
                child_config['path'] = child_config['path'].format(str(record['id']))
                total_records += sync_endpoint_refactor(client=self.generators[0].client,
                                                        catalog=self.catalog,
                                                        state=self.generators[0].state,
                                                        stream_name=child_config_key,
                                                        sub_type='self',
                                                        config=self.generators[0].config,
                                                        endpoint_config=child_config)
        return total_records
