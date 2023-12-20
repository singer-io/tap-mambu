import json
import math


class HashableDict(dict):

    @staticmethod
    def _recur_hash(value):
        if type(value) in [dict, HashableDict]:
            return HashableDict(value).__key()
        if type(value) == list:
            return tuple(sorted(map(HashableDict._recur_hash, value), key=lambda x: x if x is not None else -math.inf))
        return value

    def __key(self):
        data = [(key, self._recur_hash(value)) for key, value in self.items()]
        return tuple(sorted(data, key=lambda record: record[0]))

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, HashableDict):
            return self.__key() == other.__key()
        return NotImplemented
