import boto3
import dynamodb_operations
import json

def s3_restore(bcl_paths, execution_id, status_table):
    s3 = boto3.resource('s3')
    for x in bcl_paths:
        bcl_prefix=x['bcl_prefix']
        bcl_bucket=s3.Bucket(x['bcl_bucket'])
        restoration_list=[]    
        for obj_summary in bcl_bucket.objects.filter(Prefix=bcl_prefix):
            obj = s3.Object(obj_summary.bucket_name, obj_summary.key)
            if obj.storage_class == 'GLACIER':
                # Try to restore the object if the storage class is glacier and
                # the object does not have a completed or ongoing restoration
                # request or print out objects whose restoration is on-going.
                if obj.restore is None or 'ongoing-request="true"' in obj.restore:
                    print('Restoration in-progress: %s' % obj.key)
                    restoration_list.append(obj.key)
                    if obj.restore is None:
                        print('Submitting restoration request: %s' % obj.key)
                        obj.restore_object(RestoreRequest={'Days': 1})
                    dynamodb_operations.populate_job_details_restoring(execution_id, status_table, restoration_list, obj_summary.bucket_name)
                # Print out objects whose restoration is complete
                elif 'ongoing-request="false"' in obj.restore:
                    print('Restoration complete: %s' % obj.key)