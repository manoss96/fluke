import unittest

from fluke.auth import RemoteAuth, AWSAuth, AzureAuth, GCPAuth


class TestRemoteAuth(unittest.TestCase):

    HOST = "HOST"
    USERNAME = "USERNAME"
    PASSWORD = "PASSWORD"
    PKEY = "PATH/TO/PKEY"
    PASSPHRASE = "PASSPHRASE"
    PORT = 22
    PUBLIC_KEY = "PUBLIC_KEY"
    VERIFY_HOST = False

    def test_get_credentials_from_key(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': None,
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(
            auth.get_credentials(),
            credentials)

    def test_get_credentials_from_password(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'password': self.PASSWORD,
            'port': self.PORT,
            'public_key': None,
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_password(**credentials)
        self.assertEqual(
            auth.get_credentials(),
            credentials)
        
    def test_get_credentials_on_public_key_value(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': RemoteAuth.PublicKey.generate_ssh_rsa_key(self.PUBLIC_KEY),
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(auth.get_credentials()['public_key'].key, self.PUBLIC_KEY)

    def test_get_credentials_on_public_key_ssh_rsa_type(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': RemoteAuth.PublicKey.generate_ssh_rsa_key(self.PUBLIC_KEY),
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(
            auth.get_credentials()['public_key'].type,
            RemoteAuth.PublicKey._KeyType.SSH_RSA)
        
    def test_get_credentials_on_public_key_ssh_dss_type(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': RemoteAuth.PublicKey.generate_ssh_dss_key(self.PUBLIC_KEY),
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(
            auth.get_credentials()['public_key'].type,
            RemoteAuth.PublicKey._KeyType.SSH_DSS)
        
    def test_get_credentials_on_public_key_ssh_ed25519_type(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': RemoteAuth.PublicKey.generate_ssh_ed25519_key(self.PUBLIC_KEY),
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(
            auth.get_credentials()['public_key'].type,
            RemoteAuth.PublicKey._KeyType.SSH_ED25519)
        
    def test_get_credentials_on_public_key_ecdsa_sha2_nistp256_type(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': RemoteAuth.PublicKey.generate_ecdsa_sha2_nistp256_key(self.PUBLIC_KEY),
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(
            auth.get_credentials()['public_key'].type,
            RemoteAuth.PublicKey._KeyType.ECDSA_SHA2_NISTP256)
        
    def test_get_credentials_on_public_key_ecdsa_sha2_nistp384_type(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': RemoteAuth.PublicKey.generate_ecdsa_sha2_nistp384_key(self.PUBLIC_KEY),
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(
            auth.get_credentials()['public_key'].type,
            RemoteAuth.PublicKey._KeyType.ECDSA_SHA2_NISTP384)
        
    def test_get_credentials_on_public_key_ecdsa_sha2_nistp521_type(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'pkey': self.PKEY,
            'passphrase': self.PASSPHRASE,
            'port': self.PORT,
            'public_key': RemoteAuth.PublicKey.generate_ecdsa_sha2_nistp521_key(self.PUBLIC_KEY),
            'verify_host': self.VERIFY_HOST 
        }
        auth = RemoteAuth.from_key(**credentials)
        self.assertEqual(
            auth.get_credentials()['public_key'].type,
            RemoteAuth.PublicKey._KeyType.ECDSA_SHA2_NISTP521)
        
    def test_get_credentials_on_default_port(self):
        credentials = {
            'hostname': self.HOST,
            'username': self.USERNAME,
            'password': self.PASSWORD,
            'public_key': None,
            'verify_host': self.VERIFY_HOST
        }
        auth = RemoteAuth.from_password(**credentials)
        credentials.update({
            'port': 22
        })
        self.assertEqual(
            auth.get_credentials(),
            credentials)
        

class TestAWSAuth(unittest.TestCase):

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
        

class TestAzureAuth(unittest.TestCase):

    ACCOUNT_URL = "ACCOUNT_URL"
    TENANT_ID = "TENANT_ID"
    CLIENT_ID = "CLIENT_ID"
    CLIENT_SECRET = "CLIENT_SECRET"
    CONNECTION_STRING = "CONNECTION_STRING"
        
    def test_get_credentials_from_service_principal(self):
        credentials = {
            'account_url': self.ACCOUNT_URL,
            'tenant_id': self.TENANT_ID,
            'client_id': self.CLIENT_ID,
            'client_secret': self.CLIENT_SECRET
        }
        self.assertEqual(
            AzureAuth.from_service_principal(**credentials).get_credentials(),
            credentials)
        
    def test_get_credentials_from_connection_string(self):
        credentials = { 'conn_string': self.CONNECTION_STRING }
        self.assertEqual(
            AzureAuth.from_conn_string(**credentials).get_credentials(),
            credentials)
        

class TestGCPAuth(unittest.TestCase):

    PROJECT_ID = "PROJECT_ID"
    CREDENTIALS = "PATH/TO/CREDENTIALS/FILE"
        
    def test_get_credentials_from_application_default_credentials(self):
        self.assertEqual(
            GCPAuth.from_application_default_credentials(**{
                'project_id': self.PROJECT_ID,
                'credentials': self.CREDENTIALS,
            }).get_credentials(),
            {
                GCPAuth._PROJECT_ID: self.PROJECT_ID,
                GCPAuth._APPLICATION_DEFAULT_CREDENTIALS: self.CREDENTIALS
            })
        
    def test_get_credentials_from_service_account_key(self):
        self.assertEqual(
            GCPAuth.from_service_account_key(**{
                'credentials': self.CREDENTIALS,
            }).get_credentials(),
            {
                GCPAuth._SERVICE_ACCOUNT_KEY: self.CREDENTIALS
            })
        

if __name__=="__main__": 
     unittest.main() 