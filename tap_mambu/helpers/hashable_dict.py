import json


class HashableDict(dict):

    @staticmethod
    def _recur_hash(value):
        if type(value) in [dict, HashableDict]:
            return HashableDict(value).__key()
        if type(value) == list:
            return json.dumps(sorted(map(HashableDict._recur_hash, value)))
        return json.dumps(value)

    def __key(self):
        data = [[key, self._recur_hash(value)] for key, value in self.items()]
        return json.dumps(sorted(data, key=lambda record: record[0]))

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, HashableDict):
            return self.__key() == other.__key()
        return NotImplemented
