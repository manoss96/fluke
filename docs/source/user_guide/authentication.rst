.. _ug_authentication:

***********************
Authentication
***********************

Authentication is the process by which a user verifies their identity. All remote
resources require some form of authentication during which a user must identify
themselves so that they are granted access to said resources. Fluke itself
manages authentication through the `fluke.auth <../documentation/auth.html>`_
module. The general idea is that you must first initialize some *Auth* instance,
which is then provided to the resource with which you wish to interact.
For instance, in the code snippet below we use the
`RemoteAuth <../documentation/auth.html#fluke.auth.RemoteAuth>`_
class in order to access a file that resides within a remote server.

.. code-block:: python

  from fluke.auth import RemoteAuth
  from fluke.storage import RemoteFile

  # Create a "RemoteAuth" class instance.
  auth = RemoteAuth.from_password(
      hostname='host',
      username='user',
      password='pwd')

  # Provide it to the resource.
  with RemoteFile(auth=auth, path='/home/user/file.txt') as file:
    # You can now interact with the resource!
    file_bytes = file.read()

Below, we are going to take a look at all different types of authentication.

==========================================
Authenticating with AWS
==========================================

In order to use any AWS resource, you have to be able to authenticate with
AWS first. This can be easily achieved by creating an
`AWSAuth <../documentation/auth.html#fluke.auth.AWSAuth>`_ instance
and providing it with your access key:

.. code-block:: python

  from fluke.auth import AWSAuth

  auth = AWSAuth(
    aws_access_key_id='AWS_ACCESS_KEY_ID',
    aws_secret_access_key='AWS_SECRET_ACCESS_KEY'
  )

-----------------------------------------
Authenticating with temporary credentials
-----------------------------------------

If you have acquired any temporary security credentials,
then you also need to include the corresponding session token:

.. code-block:: python

  from fluke.auth import AWSAuth

  auth = AWSAuth(
    aws_access_key_id='AWS_ACCESS_KEY_ID',
    aws_secret_access_key='AWS_SECRET_ACCESS_KEY',
    aws_session_token='AWS_SESSION_TOKEN'
  )


-----------------------------------------
Selecting a region
-----------------------------------------

Lastly, if you need to connect to an AWS resource
that is hosted on a server within a specified region,
assuming that said resource actually exists on a server
in that region, then this is possible by setting the
``region`` parameter.

.. code-block:: python

  from fluke.auth import AWSAuth

  auth = AWSAuth(
    aws_access_key_id='AWS_ACCESS_KEY_ID',
    aws_secret_access_key='AWS_SECRET_ACCESS_KEY',
    region=AWSAuth.Region.EUROPE_WEST_1
  )

==========================================
Authenticating with Azure
==========================================

Authentication with Azure resources can happen
in two ways: either with an Azure Active Directory
(AD) service principal or with a connection string.
Below, we'll take a look at both.

---------------------------------------
Authenticating via a service principal
---------------------------------------

In order to authenticate via a service principal, you may use the
`AzureAuth.from_service_principal <../documentation/auth.html#fluke.auth.AzureAuth.from_service_principal>`_
method, providing it with the storage account's URL, as well as
all information relevant to the service principal, that is,
the *tenant ID*, *client ID* and finally the *client secret*:

.. code-block:: python

  from fluke.auth import AzureAuth

  auth = AzureAuth.from_service_principal(
      account_url = 'https://ACCOUNT.blob.core.windows.net'
      tenant_id = 'TENANT_ID',
      client_id = 'CLIENT_ID',
      client_secret = 'CLIENT_SECRET'
  )


----------------------------------------------
Authenticating via a connection string
----------------------------------------------

Alternatively, you are able to use
`AzureAuth.from_conn_string <../documentation/auth.html#fluke.auth.AzureAuth.from_conn_string>`_
so as to gain access to a specific set of Azure resources by using a connection string,
which must typically include the account's name and key, as well
as all necessary endpoints regarding the resources to which you
request access to:

.. code-block:: python

  import re
  from fluke.auth import AzureAuth

  conn_string = re.sub(
    pattern='\s',
    repl='',
    string="""
	DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;
	AccountKey=ACCOUNT_KEY;
	BlobEndpoint=BLOB_ENDPOINT
	QueueEndpoint=QUEUE_ENDPOINT;
	""")

  auth = AzureAuth.from_conn_string(conn_string)


==========================================
Authenticating with GCP
==========================================

When it comes to authenticating with GCP, there
exist two different approaches. These are via Application
Default Credentials (ADC), as well as, via a service
account key.

---------------------------------------------------
Authenicating via application default credentials
---------------------------------------------------

After having
`set up your application default credentials <https://cloud.google.com/docs/authentication/provide-credentials-adc>`_,
which results in a JSON file that contains all required credentials being generated,
you just need to invoke method `GCPAuth.from_application_default_credentials <../documentation/auth.html#fluke.auth.GCPAuth.from_application_default_credentials>`_ 
and provide it with the path pointing to said file along with the ID of the
project to which the resource you wish to access belongs:

.. code-block:: python

  from fluke.auth import GCPAuth

  auth = GCPAuth.from_application_default_credentials(
    project_id='your_project_id',
    credentials='path/to/adc/file.json'
  )


---------------------------------------------------
Authenicating via a service account key
---------------------------------------------------

Similarily, `creating a service account key <https://cloud.google.com/iam/docs/keys-create-delete>`_
also results in a JSON file containing the service account key credentials.
In this case, you just need the path pointing to this file as a service account
key will already be tied to one and only GCP project:

.. code-block:: python

  from fluke.auth import GCPAuth

  auth = GCPAuth.from_service_account_key(
    credentials='path/to/sak/file.json'
  )


==========================================
Authenticating with a remote SSH server
==========================================

There are two ways for a user to authenticate themselves while
establishing an SSH connection to a remote machine: either by
key or by password. No matter the authentication method, you
must always provide the following three bits of information:

* **hostname**: Either the name or IP address of the host, i.e the remote machine
  to which we are attempting to connect.
* **username**: The user as which you will be connecting to the remote machine.
* **port**: The port on which the connection is established. This parameter
  defaults to the value ``22`` as this is the port that is associated with
  the SSH protocol.


------------------------------------------
Authentication by key
------------------------------------------

This way of authentication has the following two prerequisites:

* There exists a public/private SSH key pair on your local machine.
* The remote machine to which you will be connecting has a copy of your public key.

If both of the above are true, then you can simply create a
`RemoteAuth <../documentation/auth.html#fluke.auth.RemoteAuth>`_ 
instance through the use of the function
`RemoteAuth.from_key <../documentation/auth.html#fluke.auth.RemoteAuth.from_key>`_,
to which you must provide the path of the file containing your private key
via the ``pkey`` parameter:

.. code-block:: python

  from fluke.auth import RemoteAuth

  auth = RemoteAuth.from_key(
    hostname='host',
    username='user',
    pkey='/home/user/.ssh/id_rsa'
  )

------------------------------------------
Authentication by password
------------------------------------------

Authenticating via password is more straightforward
as you only need to know the password of the user as
which you will be logging into the remote machine.
To do that, you simply need to invoke the function
`RemoteAuth.from_password <../documentation/auth.html#fluke.auth.RemoteAuth.from_password>`_,
providing it with said password:

.. code-block:: python

  from fluke.auth import RemoteAuth

  auth = RemoteAuth.from_password(
    hostname='host',
    username='user',
    password='pwd'
  )

------------------------------------------
Connecting to unknown hosts
------------------------------------------

By default, you are only allowed to connect to known hosts,
that is, hosts which are currently listed under your machine's
*known_hosts* file. If you ever try to establish a connection
to an unknown host, an exception will be thrown, in which case
you have two options:

#. If you are aware of the host's public key, then you may
   include this information into the ``RemoteAuth`` instance
   by generating a key via ``RemoteAuth.PublicKey``:

   .. code-block:: python

     from fluke.auth import RemoteAuth

     auth = RemoteAuth.from_password(
       hostname='host',
       username='user',
       password='pwd',
       public_key=RemoteAuth.PublicKey.generate_ssh_rsa_key('PUBLIC_RSA_KEY')
     )

#. Alternatively, you may simply set parameter ``verify_host`` to ``False``:

   .. code-block:: python

     from fluke.auth import RemoteAuth

     auth = RemoteAuth.from_password(
       hostname='host',
       username='user',
       password='pwd',
       verify_host=False
     )

Note, however, that the second option is not generally recommended
as it renders your machine vulnerable to
`Man-in-the-Middle attacks <https://en.wikipedia.org/wiki/Man-in-the-middle_attack>`_.