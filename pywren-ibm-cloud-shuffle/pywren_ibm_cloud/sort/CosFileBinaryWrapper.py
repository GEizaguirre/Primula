import io


class CosFileBinaryWrapper(io.RawIOBase):

    def __init__(self, cos_client, bucket_name, key):
        self.cos_client = cos_client
        self.cos_object = cos_client.get_object(Bucket=bucket_name, Key=key)
        self.current_read_position = 0
        self.bucket_name = bucket_name
        self.key = key

    @property
    def size(self):
        return self.cos_object['ContentLength']

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.current_read_position = offset
        elif whence == io.SEEK_CUR:
            self.current_read_position += offset
        elif whence == io.SEEK_END:
            self.current_read_position = self.size + offset
        else:
            raise ValueError("invalid whence (%r, should be %d, %d, %d)" % (
                whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END
            ))
        return self.current_read_position

    def seekable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            # Read to the end of the file
            range_header = "bytes=%d-" % self.current_read_position
            self.seek(offset=0, whence=io.SEEK_END)
        else:
            new_position = self.current_read_position + size

            # If we're going to read beyond the end of the object, return
            # the entire object.
            if new_position >= self.size:
                return self.read()

            range_header = "bytes=%d-%d" % (self.current_read_position, new_position - 1)
            self.seek(offset=size, whence=io.SEEK_CUR)

        #return self.cos_object.get(Range=range_header)["Body"].read()
        return self.cos_client.get_object(Bucket=self.bucket_name, Key=self.key, Range=range_header)["Body"].read()



    def readable(self):
        return True
