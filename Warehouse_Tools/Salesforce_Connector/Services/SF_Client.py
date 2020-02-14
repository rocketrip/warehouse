import os
from simple_salesforce import Salesforce as Sf


class Salesforce(Sf):
    def __init__(self, **kwargs):
        creds = {"username": kwargs.get("username",
                                        os.getenv("SALESFORCE_USERNAME")),
                 "password": kwargs.get("password",
                                        os.getenv("SALESFORCE_PASSWORD")),
                 "security_token": kwargs.get("security_token",
                                              os.getenv("SALESFORCE_SECURITY_TOKEN"))
                 }
        super(Salesforce, self).__init__(**creds)

    def execute_query(self, query):
        res = self.query_all(query)
        return res
