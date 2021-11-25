import time
import requests

from typing import List
from .. import TapGenerator


class LoanAccountsGenerator(TapGenerator):
    def __init__(self, *args, **kwargs):
        super(LoanAccountsGenerator, self).__init__(*args, **kwargs)
        # self.__init_buffers()

    # def __init_buffers(self):
    #     super(LoanAccountsGenerator, self).__init_buffers()
    #     self.buffer_ad: List = list()
    #     self.buffer_lm: List = list()
