"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os

from tap_tester import connections, menagerie, runner


class MambuBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-mambu"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.mambu"

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_metadata(self):
        return {
            "branches": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "cards": {
                self.PRIMARY_KEYS: {
                    "deposit_id",
                    "reference_token"
                },
                self.REPLICATION_METHOD: "FULL_TABLE",
            },
            "communications": {
                self.PRIMARY_KEYS: {
                    "encoded_key"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "creation_date"
                }
            },
            "centres": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "clients": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "credit_arrangements": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "custom_field_sets": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "FULL_TABLE",
            },
            "deposit_accounts": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "deposit_products": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "deposit_transactions": {
                self.PRIMARY_KEYS: {
                    "encoded_key"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "creation_date"
                }
            },
            "groups": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "loan_accounts": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "loan_repayments": {
                self.PRIMARY_KEYS: {
                    "encoded_key"
                },
                self.REPLICATION_METHOD: "FULL_TABLE",
            },
            "loan_products": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "loan_transactions": {
                self.PRIMARY_KEYS: {
                    "encoded_key"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "creation_date"
                }
            },
            "tasks": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "users": {
                self.PRIMARY_KEYS: {
                    "id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "gl_accounts": {
                self.PRIMARY_KEYS: {
                    "gl_code"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_modified_date"
                }
            },
            "gl_journal_entries": {
                self.PRIMARY_KEYS: {
                    "entry_id"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "booking_date"
                }
            },
            "activities": {
                self.PRIMARY_KEYS: {
                    "encoded_key"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "timestamp"
                }
            },
            "index_rate_sources": {
                self.PRIMARY_KEYS: {
                    "encoded_key"
                },
                self.REPLICATION_METHOD: "FULL_TABLE",
            },
            "installments": {
                self.PRIMARY_KEYS: {
                    "encoded_key"
                },
                self.REPLICATION_METHOD: "INCREMENTAL",
                self.REPLICATION_KEYS: {
                    "last_paid_date"
                }
            }
        }


    def create_connection(self, original_properties: bool = True):
        """Create a new connection with the test name"""
        # Create the connection
        conn_id = connections.ensure_connection(self, original_properties)

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        return conn_id

    def get_properties(self):
        return {
            'start_date': '2020-10-01T00:00:00Z',
            "username": os.environ['TAP_MAMBU_USERNAME'],
            "subdomain": os.environ['TAP_MAMBU_SUBDOMAIN']
            }

    def get_credentials(self):
        return {
            "password": os.environ['TAP_MAMBU_PASSWORD']
            }

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}
