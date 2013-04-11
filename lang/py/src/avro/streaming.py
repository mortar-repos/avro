try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO

from avro import io
from avro import schema
from avro.datafile import DataFileWriter

VERSION = 1
MAGIC = 'Obj' + chr(VERSION)
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
SYNC_INTERVAL = 1000 * SYNC_SIZE
META_SCHEMA = schema.parse("""\
{"type": "record", "name": "org.apache.avro.file.Header",
 "fields" : [
   {"name": "magic", "type": {"type": "fixed", "name": "magic", "size": %d}},
   {"name": "meta", "type": {"type": "map", "values": "bytes"}},
   {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": %d}}]}
""" % (MAGIC_SIZE, SYNC_SIZE))

SCHEMA_KEY = "avro.schema"

class StreamingBinaryDecoder(io.BinaryDecoder):
  def read_long(self):
    """
    Same as superclass read_long except check for EOF
    We use this to signal DataStreamReader to stop iteration:
    DataStreamReader calls this to read the (count, length) info
    at the start of each block, so if this throws EOFError, it raises StopIteration
    """
    c = self.read(1)
    if len(c) == 0:
      raise EOFError

    b = ord(c)
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
      b = ord(self.read(1))
      n |= (b & 0x7F) << shift
      shift += 7
    datum = (n >> 1) ^ -(n & 1)
    return datum

  def skip(self, n):
    self.read(n)

class DataStreamWriter(DataFileWriter):
  # Remove support for compression, since DataStreamReader doesn't support it
  # Remove support for "append mode", since regular stream writing is appending
  def __init__(self, writer, datum_writer, writers_schema):
    self._writer = writer
    self._encoder = io.BinaryEncoder(writer)
    self._datum_writer = datum_writer
    self._buffer_writer = StringIO()
    self._buffer_encoder = io.BinaryEncoder(self._buffer_writer)
    self._block_count = 0
    self._meta = {}
    self._header_written = False

    self._sync_marker = DataFileWriter.generate_sync_marker()
    self.set_meta('avro.codec', 'null')
    self.set_meta('avro.schema', str(writers_schema))
    self.datum_writer.writers_schema = writers_schema

class DataStreamReader(object):
  def __init__(self, reader, datum_reader):
    self.reader = reader
    self.decoder = StreamingBinaryDecoder(reader)
    self.datum_reader = datum_reader
    
    # read the header: magic, meta, sync
    self._read_header()

    # get ready to read
    self.block_count = 0
    self.first_block = True
    self.datum_reader.writers_schema = schema.parse(self._meta.get(SCHEMA_KEY))

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    # Perform a close if there's no exception
    if type is None:
      self.close()

  def __iter__(self):
    return self

  def _read_header(self):
    # read header into a dict
    header = self.datum_reader.read_data(
      META_SCHEMA, META_SCHEMA, self.decoder)

    # check magic number
    if header.get('magic') != MAGIC:
      fail_msg = "Not an Avro data file: %s doesn't match %s."\
                 % (header.get('magic'), MAGIC)
      raise schema.AvroException(fail_msg)

    # set metadata
    self._meta = header['meta']

    # ignore header['sync'], since we only accept uncompressed data

  def _read_block_header(self):
    # Will throw EOFError if at EOF
    self.block_count = self.decoder.read_long()
    # Skip a long; we don't need to use the length.
    self.decoder.skip_long()

  def next(self):
    if self.block_count == 0:
      # Skip sync markers (we only support uncompressed data)
      if self.first_block:
          self.first_block = False
      else:
        self.reader.read(SYNC_SIZE)

      try:
        self._read_block_header()
      except EOFError:
        raise StopIteration

    datum = self.datum_reader.read(self.decoder) 
    self.block_count -= 1
    return datum

  def close(self):
    self.reader.close()
