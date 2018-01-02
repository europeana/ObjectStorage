# ObjectStorage

ObjectStorage is a library that takes care of reading and writing data to a storage service provider. It provides a 
StorageObject interface which other components can use without worrying about how to read or write the object. 
ObjectStorage is used by the Europeana Search & Record API for retrieving images and by the Sitemap project for storing
and retrieving sitemap xml files.

At the moment ObjectStorage supports Amazon S3 and Swift.
For more information see: https://aws.amazon.com/documentation/s3/ or http://docs.openstack.org/developer/swift/