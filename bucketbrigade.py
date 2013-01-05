from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto
from Queue import LifoQueue
import json
import threading
import logging

"""

Simple job utility for S3 buckets 

"""
logging.basicConfig(level=logging.DEBUG)
class BucketCopyWorker(threading.Thread):
    def __init__(self,  queue, \
                        srcBucket, tgtBucket,\
                        srcPrefix, tgtPrefix,\
                        ownerBucket=None, ownerID=None,\
                        stopExcept=True):
        threading.Thread.__init__(self)
        
        self.srcBucket = srcBucket
        self.tgtBucket = tgtBucket
        
        self.srcPrefix = srcPrefix
        self.tgtPrefix = tgtPrefix
        
        self.ownerBucket = ownerBucket
        self.ownerID = ownerID
        
        self.queue = queue
        self.stopExcept = stopExcept
        
    def run(self):
        while True:
            src_key_name = self.queue.get()
            src_key = Key(self.srcBucket, src_key_name)
            tgt_key_name = src_key_name.replace( self.srcPrefix, self.tgtPrefix, 1 )
            tgt_key = Key( self.tgtBucket, tgt_key_name )
            logging.debug( 'stat: ' + src_key_name )
            if (not tgt_key.exists() ) or src_key.etag != tgt_key.etag:
                logging.info( 'copy: ' + src_key.key )
                #try to copy as is
                try:
                    self.tgtBucket\
                        .copy_key(  tgt_key.key, \
                                    self.srcBucket.name, \
                                    src_key.key, \
                                    storage_class=src_key.storage_class)
                except:
                    #uh-ohes
                    logging.warn('owner-acl: copy fail for %s' % src_key.key)
                    if not self.ownerBucket:
                        if self.stopExcept:
                            raise
                        self.queue.task_done()
                        continue
                    
                    #allowed to change acls yay
                    logging.warn('owner-acl: trying to change acl for %s' % src_key.key)
                    try:
                        #let's try changing some acl ova here
                        self.ownerBucket\
                            .get_key(src_key.key)\
                            .add_user_grant( 'READ' , self.ownerID)
                    except:
                        logging.warn('owner-acl: failed to change acl for %s' % src_key.key)
                        if self.stopExcept:
                            raise
                    try:
                        #ok now try to copy again (2nd try)
                        self.tgtBucket\
                            .copy_key(  tgt_key.key,\
                                        self.srcBucket.name,\
                                        src_key.key,\
                                        storage_class=src_key.storage_class)
                    except:
                        logging.warn('owner-acl: give up, failed twice for %s' % src_key.key)
                        if self.stopExcept:
                            raise
            else:
                logging.info( 'exists and etag matches: ' + src_key.key )
            self.queue.task_done()



class BucketBrigadeJob:
    def __init__(self, conf_file=None, parallel=1):
        self.parallel = parallel
        if conf_file:
            logging.info('Loading job from: %s' % conf_file )
            self.parse_conf( open(conf_file,'r') )
            return
        else:
            raise Exception("Currently BucketBrigadeJob can only be used with a config file!")
    def parse_conf( self, cfh ):
        logging.info( str(self.__dict__) )
        cvals = json.load( cfh )
        logging.info(str(cvals))
        self.__dict__.update( cvals )
        logging.info( str(self.__dict__) )
    def get_conn( self, user ):
        if not user in self.users:
            raise Exception("Requested user: %s not in available users %s" % 
                                    ( user, str(self.users.keys() ) ) )
        cred = self.users[user]
        return S3Connection( cred['key'], cred['secret-key'] )
    def add_read_acl( self, bucket_conf, user ):
        if not user in self.users:
            raise Exception("Requested user: %s not in available users %s" % 
                                    ( user, str(self.users.keys() ) ) )
        bucket = self.get_conn( bucket_conf['owner'] ).get_bucket( bucket_conf['bucket'] )
        reader = self.users[user]
        if reader.has_key('canonical-id'):
            bucket.add_user_grant( 'READ', reader['canonical-id'], recursive=True )
        elif reader.has_key('email-id'):
            bucket.add_email_grant( 'READ', reader['email-id'], recursive=True )
        else:
            raise Exception("Could not find canonical-id or email-id to add read acl for user")
    def copy_job(self, max_keys=1000):
        logging.info( 'start copy_bucket' )
        src = self.job['source']
        tgt = self.job['target']
        
        conn = self.get_conn( tgt['owner'] )
        srcBucket = conn.get_bucket( src['bucket'] )
        tgtBucket = conn.get_bucket( tgt['bucket'] )
        
        if self.job['options']['allow-acl-change']:
            ownerBucketView = self.get_conn( src['owner'] ).get_bucket( src['bucket'] )
            ownerID = self.users[ tgt['owner'] ]['canonical-id']
        else:
            ownerBucketView = None
            ownerID = None
        resultMarker = ''
        q = LifoQueue(maxsize=5000)
        for i in range(self.parallel):
            logging.info( 'adding worker %d' % i )
            t = BucketCopyWorker(q, srcBucket, tgtBucket, src['key-prefix'], tgt['key-prefix'],  ownerBucketView, ownerID)
            t.daemon = True
            t.start()
        while True:
            logging.info( 'fetch next 1000, backlog currently at %i' % q.qsize() )
            keys = srcBucket.get_all_keys( prefix=src['key-prefix'], max_keys=max_keys, marker = resultMarker)
            for k in keys:
                q.put(k.key)
            if len(keys) < max_keys:
                print 'Done'
                break
            resultMarker = keys[maxKeys - 1].key
        q.join()
        logging.info( 'done copy_bucket' )
    def run(self):
        supported = ['sync']
        if self.job['type'] in ['sync','copy']:
            self.copy_job()
        else:
            raise Exception( "Currently supporting just the following job types: %s" % str(supported) )
