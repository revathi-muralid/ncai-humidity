import sys
import argparse
import boto3

def aws_s3_nfiles( bucket_name, extension, prefix: str = None ):

        resource = boto3.resource('s3')
        bucket   = resource.Bucket( bucket_name )
        #bucket = resource.Object(bucket_name='ncai-humidity', key='HIRS')
        objects  = (
            bucket.objects.filter(Prefix=prefix)
            if isinstance(prefix, str) else
            bucket.objects.all()
        )

        files    = ['']
        for obj in objects:
            for part in obj.key.split('/'):
                if part.endswith( extension ) and files[-1] != part:
                    files.append( part )

        return files

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument( 'bucket', type=str, help='Name of bucket to count files in')               
    parser.add_argument( 'extension', type=str, help='Extensions of files to look for')
    parser.add_argument( '--prefix', type=str, help='Key prefix to filter by')

    args = parser.parse_args()

    nfiles = aws_s3_nfiles( args.bucket, args.extension, prefix=args.prefix )
    print(
        f'There are {nfiles} files in the "{args.bucket}" bucket '
        f'with extension "{args.extension}"'
    )
    sys.exit()
