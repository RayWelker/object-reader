import boto3
import json

def s3_restore(bcl_paths):
    client = boto3.client('s3')
    s3 = boto3.resource('s3')
    for x in bcl_paths:
        bcl_prefix=x['bcl_prefix']
        bcl_bucket=s3.Bucket(x['bcl_bucket'])
        for obj_summary in bcl_bucket.objects.filter(Prefix=bcl_prefix):
            obj = s3.Object(obj_summary.bucket_name, obj_summary.key)
            if obj.storage_class == 'GLACIER':
                # Try to restore the object if the storage class is glacier and
                # the object does not have a completed or ongoing restoration
                # request.
                if obj.restore is None:
                    print('Submitting restoration request: %s' % obj.key)
                    obj.restore_object(RestoreRequest={'Days': 1})
                # Print out objects whose restoration is on-going
                elif 'ongoing-request="true"' in obj.restore:
                    print('Restoration in-progress: %s' % obj.key)
                # Print out objects whose restoration is complete
                elif 'ongoing-request="false"' in obj.restore:
                    print('Restoration complete: %s' % obj.key)