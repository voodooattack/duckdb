/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef avro_Generic_hh__
#define avro_Generic_hh__

#include "Config.hh"
#include "Decoder.hh"
#include "GenericDatum.hh"
#include "Types.hh"

namespace avro {
/**
 * A utility class to read generic datum from decoders.
 */
class GenericReader {
    const ValidSchema schema_;
    const bool isResolving_;
    const DecoderPtr decoder_;

    static void read(GenericDatum &datum, Decoder &d, bool isResolving);

public:
    /**
     * Constructs a reader for the given schema using the given decoder.
     */
    GenericReader(ValidSchema s, const DecoderPtr &decoder);

    /**
     * Constructs a reader for the given reader's schema \c readerSchema
     * using the given
     * decoder which holds data matching writer's schema \c writerSchema.
     */
//    GenericReader(const ValidSchema &writerSchema,
//                  const ValidSchema &readerSchema, const DecoderPtr &decoder);

    GenericReader(const GenericReader &) = delete;
    GenericReader &operator=(const GenericReader &) = delete;

    /**
     * Reads a value off the decoder.
     */
    void read(GenericDatum &datum) const;

    /**
     * Drains any residual bytes in the input stream (e.g. because
     * reader's schema has no use of them) and return unused bytes
     * back to the underlying input stream.
     */
    void drain() {
        decoder_->drain();
    }
    /**
     * Reads a generic datum from the stream, using the given schema.
     */
    static void read(Decoder &d, GenericDatum &g);

    /**
     * Reads a generic datum from the stream, using the given schema.
     */
    static void read(Decoder &d, GenericDatum &g, const ValidSchema &s);
};

template<typename T>
struct codec_traits;

/**
 * Specialization of codec_traits for Generic datum along with its schema.
 * This is maintained for compatibility with old code. Please use the
 * cleaner codec_traits<GenericDatum> instead.
 */
template<>
struct codec_traits<std::pair<ValidSchema, GenericDatum>> {
    /** Decodes */
    static void decode(Decoder &d, std::pair<ValidSchema, GenericDatum> &p) {
        GenericReader::read(d, p.second, p.first);
    }
};

/**
 * Specialization of codec_traits for GenericDatum.
 */
template<>
struct codec_traits<GenericDatum> {
    /** Decodes */
    static void decode(Decoder &d, GenericDatum &g) {
        GenericReader::read(d, g);
    }
};

} // namespace avro
#endif
