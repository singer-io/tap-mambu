from abc import abstractmethod

from tap_mambu.tap_mambu_refactor.tap_generators.generator import TapGenerator


class GlAccountsGenerator(TapGenerator):
    @abstractmethod
    def _init_endpoint_config(self):
        super(TapGenerator, self)._init_endpoint_config()
        self.endpoint_path = "glaccounts"
        self.endpoint_api_method = "GET"
        self.endpoint_bookmark_field = "lastModifiedDate"


class GLAccountsAssetGenerator(GlAccountsGenerator):
    def _init_endpoint_config(self):
        self.endpoint_params["type"] = "ASSET"


class GLAccountsLiabilityGenerator(GlAccountsGenerator):
    def _init_endpoint_config(self):
        self.endpoint_params["type"] = "ASSET"


class GLAccountsEquityGenerator(GlAccountsGenerator):
    def _init_endpoint_config(self):
        self.endpoint_params["type"] = "ASSET"


class GLAccountsIncomeGenerator(GlAccountsGenerator):
    def _init_endpoint_config(self):
        self.endpoint_params["type"] = "ASSET"


class GLAccountsExpenseGenerator(GlAccountsGenerator):
    def _init_endpoint_config(self):
        self.endpoint_params["type"] = "ASSET"