import os
from contextlib import contextmanager


class SecretKeeperException(Exception):
    pass


class SecretKeeper:
    def __init__(self, additional_secrets=None):
        self.principal_secrets = os.environ
        if additional_secrets is None:
            additional_secrets = []
        self.additional_secrets = additional_secrets

    def get(self, key):
        for secret_registry in reversed(self.additional_secrets):
            secret = secret_registry.get(key)
            if secret is not None:
                return secret
        try:
            value = self.principal_secrets[key]
        except KeyError as e:
            raise SecretKeeperException(f"No secret key `{key}` found") from e
        return value

    def __getitem__(self, key):
        return self.get(key)

    @contextmanager
    def with_secrets(self, **kwargs):
        self.additional_secrets.append(kwargs)
        try:
            yield
        finally:
            self.additional_secrets = self.additional_secrets[:-1]


secret_keeper = SecretKeeper()
