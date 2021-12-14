/**
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

#include "Generic.hh"
#include <utility>

namespace avro {

using std::ostringstream;
using std::string;
using std::vector;

typedef vector<uint8_t> bytes;

void GenericContainer::assertType(const NodePtr &schema, Type type) {
    if (schema->type() != type) {
        throw duckdb::InvalidInputException("Schema type %s expected %s", toString(schema->type()), toString(type));
    }
}

GenericReader::GenericReader(ValidSchema s, const DecoderPtr &decoder) : schema_(std::move(s)), isResolving_(false), //dynamic_cast<ResolvingDecoder *>(&(*decoder)) != nullptr),
                                                                         decoder_(decoder) {
}

//GenericReader::GenericReader(const ValidSchema &writerSchema,
//                             const ValidSchema &readerSchema, const DecoderPtr &decoder) : schema_(readerSchema),
//                                                                                           isResolving_(true),
//                                                                                           decoder_(resolvingDecoder(writerSchema, readerSchema, decoder)) {
//}

void GenericReader::read(GenericDatum &datum) const {
    datum = GenericDatum(schema_.root());
    read(datum, *decoder_, isResolving_);
}

void GenericReader::read(GenericDatum &datum, Decoder &d, bool isResolving) {
    if (datum.isUnion()) {
        datum.selectBranch(d.decodeUnionIndex());
    }
    switch (datum.type()) {
        case AVRO_NULL:
            d.decodeNull();
            break;
        case AVRO_BOOL:
            datum.value<bool>() = d.decodeBool();
            break;
        case AVRO_INT:
            datum.value<int32_t>() = d.decodeInt();
            break;
        case AVRO_LONG:
            datum.value<int64_t>() = d.decodeLong();
            break;
        case AVRO_FLOAT:
            datum.value<float>() = d.decodeFloat();
            break;
        case AVRO_DOUBLE:
            datum.value<double>() = d.decodeDouble();
            break;
        case AVRO_STRING:
            d.decodeString(datum.value<string>());
            break;
        case AVRO_BYTES:
            d.decodeBytes(datum.value<bytes>());
            break;
        case AVRO_FIXED: {
            auto &f = datum.value<GenericFixed>();
            d.decodeFixed(f.schema()->fixedSize(), f.value());
        } break;
        case AVRO_RECORD: {
            auto &r = datum.value<GenericRecord>();
            size_t c = r.schema()->leaves();
            ////if (isResolving) {
            ////    std::vector<size_t> fo =
            ////        static_cast<ResolvingDecoder &>(d).fieldOrder();
            ////    for (size_t i = 0; i < c; ++i) {
            ////        read(r.fieldAt(fo[i]), d, isResolving);
            ////    }
            //} else {
                for (size_t i = 0; i < c; ++i) {
                    read(r.fieldAt(i), d, isResolving);
                //}
            }
        } break;
        case AVRO_ENUM:
            datum.value<GenericEnum>().set(d.decodeEnum());
            break;
        case AVRO_ARRAY: {
            auto &v = datum.value<GenericArray>();
            vector<GenericDatum> &r = v.value();
            const NodePtr &nn = v.schema()->leafAt(0);
            r.resize(0);
            size_t start = 0;
            for (size_t m = d.arrayStart(); m != 0; m = d.arrayNext()) {
                r.resize(r.size() + m);
                for (; start < r.size(); ++start) {
                    r[start] = GenericDatum(nn);
                    read(r[start], d, isResolving);
                }
            }
        } break;
        case AVRO_MAP: {
            auto &v = datum.value<GenericMap>();
            GenericMap::Value &r = v.value();
            const NodePtr &nn = v.schema()->leafAt(1);
            r.resize(0);
            size_t start = 0;
            for (size_t m = d.mapStart(); m != 0; m = d.mapNext()) {
                r.resize(r.size() + m);
                for (; start < r.size(); ++start) {
                    d.decodeString(r[start].first);
                    r[start].second = GenericDatum(nn);
                    read(r[start].second, d, isResolving);
                }
            }
        } break;
        default:
            throw duckdb::InvalidInputException("Unknown schema type %s", toString(datum.type()));
    }
}

void GenericReader::read(Decoder &d, GenericDatum &g, const ValidSchema &s) {
    g = GenericDatum(s);
    read(d, g);
}

void GenericReader::read(Decoder &d, GenericDatum &g) {
    read(g, d, false);//dynamic_cast<ResolvingDecoder *>(&d) != nullptr);
}

} // namespace avro
