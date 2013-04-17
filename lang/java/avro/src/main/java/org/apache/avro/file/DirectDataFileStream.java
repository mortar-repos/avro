/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;

/**
 * Variant of DataFileStream that uses DirectBinaryStreamDecoder
 * instead of BinaryDecoder, as the latter is bugged when
 * being piped data via stdin from another process.
 */
public class DirectDataFileStream<D> extends DataFileStream<D> {
  public DirectDataFileStream(InputStream in, DatumReader<D> reader) throws IOException {
    super(in, reader);
  }

  /** Initialize the stream by reading from its head. */
  void initialize(InputStream in) throws IOException {
    this.header = new DataFileStream.Header();
    this.vin = DecoderFactory.get().directBinaryStreamDecoder(in, vin);
    byte[] magic = new byte[DataFileConstants.MAGIC.length];
    try {
      vin.readFixed(magic);                         // read magic
    } catch (IOException e) {
      throw new IOException("Not a data file.");
    }
    if (!Arrays.equals(DataFileConstants.MAGIC, magic))
      throw new IOException("Not a data file.");

    long l = vin.readMapStart();                   // read meta data
    if (l > 0) {
      do {
        for (long i = 0; i < l; i++) {
          String key = vin.readString(null).toString();
          ByteBuffer value = vin.readBytes(null);
          byte[] bb = new byte[value.remaining()];
          value.get(bb);
          header.meta.put(key, bb);
          header.metaKeyList.add(key);
        }
      } while ((l = vin.mapNext()) != 0);
    }
    vin.readFixed(header.sync);                    // read sync
    
    // finalize the header
    header.metaKeyList = Collections.unmodifiableList(header.metaKeyList);
    header.schema = Schema.parse(getMetaString(DataFileConstants.SCHEMA),false);
    this.codec = resolveCodec();
    reader.setSchema(header.schema);
  }
}

