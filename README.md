Bucket Brigade
==========================
Allows for simple copying/syncing of buckets/keys across S3 accounts.

Allows automatic ACL modification where it is required (allow READ etc).

It's also multi-threaded so we can deal with big big buckets. 

Get started:

    $ git clone http://github.com/uhjish/bucketbrigade
    $ cd bucketbrigade
    $ python setup.py install
    $ b2b.py --help

Examples
-------------------
Look at sync_example.job for an idea of how a sync job can be setup.

We use this to set up crons and to sync production to devel etc.

Finding AWS IDs
-------------------
Client/Provider gives you access to a bucket with key, secret-key
but you need to get their CanonicalID to start mucking around with ACLs

Here's where you'd find it: http://www.bucketexplorer.com/awsutility.aspx

Not the happiest most idealist place, but hey it works.

References
----------
- Paul Tuckey: https://github.com/paultuckey/s3-bucket-to-bucket-copy-py
- Boto: https://github.com/boto/boto





