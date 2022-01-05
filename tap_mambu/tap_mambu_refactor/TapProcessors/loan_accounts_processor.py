from .processor import TapProcessor


class LoanAccountsProcessor(TapProcessor):
    def __process_child_records(self, record):
        super().__process_child_records(self, record)

        from .. import sync_endpoint_refactor
        children_configs = self.generators[0].endpoint_config.get('children', {})
        for child_config_key in children_configs:
            child_config = children_configs[child_config_key]
            child_config = dict(child_config)
            child_config.get('path').format(record['id'])
            sync_endpoint_refactor(client=self.generators[0].client,
                                   catalog=self.catalog,
                                   state=self.generators[0].state,
                                   stream_name=child_config_key,
                                   sub_type='',
                                   config=self.generators[0].config,
                                   endpoint_config=child_config)
