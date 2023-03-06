from unittest import TestCase


from fluke.auth import RemoteAuth, AWSAuth, AzureAuth


class TestRemoteAuth(TestCase):

    HOST = "HOST"
    USERNAME = "USERNAME"
    PASSWORD = "PASSWORD"
    PKEY = "PATH/TO/PKEY"
    PASSPHRASE = "PASSPHRASE"
    PORT = 22
    PUBLIC_KEY = "PUBLIC_KEY"
    KEY_TYPE = RemoteAuth.KeyType.SSH_RSA
    VERIFY_HOST = False

    def test_get_credentials_from_key(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': self.PUBLIC_KEY,
            'key_type': self.KEY_TYPE,
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        credentials.update({'key_type': credentials['key_type'].value})
        self.assertEqual(
            auth.get_credentials(),
            credentials)

    def test_get_credentials_from_password(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'password': self.PASSWORD,
            'port': self.PORT,
            'public_key': self.PUBLIC_KEY,
            'key_type': self.KEY_TYPE,
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_password(**credentials)
        credentials.update({'key_type': credentials['key_type'].value})
        self.assertEqual(
            auth.get_credentials(),
            credentials)
        
    def test_get_credentials_on_default_port(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'password': self.PASSWORD,
            'public_key': self.PUBLIC_KEY,
            'key_type': self.KEY_TYPE,
            'verify_host': self.VERIFY_HOST
        }
        auth = RemoteAuth.from_password(**credentials)
        credentials.update({
            'port': 22,
            'key_type': credentials['key_type'].value
        })
        self.assertEqual(
            auth.get_credentials(),
            credentials)
        

class TestAWSAuth(TestCase):

    AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
    AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
    AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"
    REGION = AWSAuth.Region.EUROPE_WEST_1

    def test_get_credentials(self):
        credentials = {
            'aws_access_key_id': self.AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': self.AWS_SECRET_ACCESS_KEY,
            'aws_session_token': self.AWS_SESSION_TOKEN,
            'region': self.REGION
        }
        auth = AWSAuth(**credentials)
        credentials.update({'region_name': credentials.pop('region').value})
        self.assertEqual(
            auth.get_credentials(),
            credentials)
        

class TestAzureAuth(TestCase):

    ACCOUNT_URL = "ACCOUNT_URL"
    TENANT_ID = "TENANT_ID"
    CLIENT_ID = "CLIENT_ID"
    CLIENT_SECRET = "CLIENT_SECRET"
    CONNECTION_STRING = "CONNECTION_STRING"

    def test_get_credentials(self):
        credentials = {
            'account_url': self.ACCOUNT_URL,
            'tenant_id': self.TENANT_ID,
            'client_id': self.CLIENT_ID,
            'client_secret': self.CLIENT_SECRET
        }
        self.assertEqual(
            AzureAuth(**credentials).get_credentials(),
            credentials)
        
    def test_from_connection_string(self):
        credentials = { 'conn_string': self.CONNECTION_STRING }
        self.assertEqual(
            AzureAuth.from_conn_string(**credentials).get_credentials(),
            credentials)