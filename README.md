# _Beansdump_

[![Build Status](https://travis-ci.org/fangli/beansdump.png?branch=master)](https://travis-ci.org/fangli/beansdump)

_Description:_ Beansdump is a data collecting tool which reserving jobs from beanstalkd and dumping them into AWS S3 in bulk.
Beansdump WILL LOSE DATA (usually dozens of jobs) since it has two levels of memory cache built-in when restart.

It's currently in developing stage, use it at your own risk.

## Install and Compile

No addition steps to compile beansdump, all things you need to do are just a `go build`

1. _git clone https://github.com/fangli/beansdump_
2. _go build beansdump.go_

## Usage

    -s="localhost:11300": IP:Port point to beanstalkd server (default to localhost:11300)
    -t="default": Message tube for metrics (default to 'default')

    -u="": The S3 accesskey, required
    -p="": The S3 secret, required
    -r="https://mybucket.s3.amazonaws.com/test/": Point to the folder URL of target S3 bucket

    -f="/mnt/": The tmp dir to cache the S3 file, default to /mnt/
    -i=300: The interval to cache the jobs and send to S3 in bulk, default to 300(s)
    
*Example:*

`./beansdump -f /tmp/ -i 30 -u "MY_S3_ACCESS_KEY" -p "MY_S3_SECRET" -r "https://mybucket.s3.amazonaws.com/test/"`

## Contributing changes

- Initial release

## License

See `LICENCE.txt`

