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

#include "NodeImpl.hh"
#include <sstream>
#include <utility>

using std::string;
namespace avro {

const int kByteStringSize = 6;

SchemaResolution
NodePrimitive::resolve(const Node &reader) const {
    if (type() == reader.type()) {
        return RESOLVE_MATCH;
    }

    switch (type()) {

        case AVRO_INT:

            if (reader.type() == AVRO_LONG) {
                return RESOLVE_PROMOTABLE_TO_LONG;
            }

            // fall-through intentional

        case AVRO_LONG:

            if (reader.type() == AVRO_FLOAT) {
                return RESOLVE_PROMOTABLE_TO_FLOAT;
            }

            // fall-through intentional

        case AVRO_FLOAT:

            if (reader.type() == AVRO_DOUBLE) {
                return RESOLVE_PROMOTABLE_TO_DOUBLE;
            }

        default: break;
    }

    return furtherResolution(reader);
}

SchemaResolution
NodeRecord::resolve(const Node &reader) const {
    if (reader.type() == AVRO_RECORD) {
        if (name() == reader.name()) {
            return RESOLVE_MATCH;
        }
    }
    return furtherResolution(reader);
}

SchemaResolution
NodeEnum::resolve(const Node &reader) const {
    if (reader.type() == AVRO_ENUM) {
        return (name() == reader.name()) ? RESOLVE_MATCH : RESOLVE_NO_MATCH;
    }
    return furtherResolution(reader);
}

SchemaResolution
NodeArray::resolve(const Node &reader) const {
    if (reader.type() == AVRO_ARRAY) {
        const NodePtr &arrayType = leafAt(0);
        return arrayType->resolve(*reader.leafAt(0));
    }
    return furtherResolution(reader);
}

SchemaResolution
NodeMap::resolve(const Node &reader) const {
    if (reader.type() == AVRO_MAP) {
        const NodePtr &mapType = leafAt(1);
        return mapType->resolve(*reader.leafAt(1));
    }
    return furtherResolution(reader);
}

SchemaResolution
NodeUnion::resolve(const Node &reader) const {

    // If the writer is union, resolution only needs to occur when the selected
    // type of the writer is known, so this function is not very helpful.
    //
    // In this case, this function returns if there is a possible match given
    // any writer type, so just search type by type returning the best match
    // found.

    SchemaResolution match = RESOLVE_NO_MATCH;
    for (size_t i = 0; i < leaves(); ++i) {
        const NodePtr &node = leafAt(i);
        SchemaResolution thisMatch = node->resolve(reader);
        if (thisMatch == RESOLVE_MATCH) {
            match = thisMatch;
            break;
        }
        if (match == RESOLVE_NO_MATCH) {
            match = thisMatch;
        }
    }
    return match;
}

SchemaResolution
NodeFixed::resolve(const Node &reader) const {
    if (reader.type() == AVRO_FIXED) {
        return (
                   (reader.fixedSize() == fixedSize()) && (reader.name() == name()))
            ? RESOLVE_MATCH
            : RESOLVE_NO_MATCH;
    }
    return furtherResolution(reader);
}

SchemaResolution
NodeSymbolic::resolve(const Node &reader) const {
    const NodePtr &node = leafAt(0);
    return node->resolve(reader);
}

NodeRecord::NodeRecord(const HasName &name,
                       const MultiLeaves &fields,
                       const LeafNames &fieldsNames,
                       std::vector<GenericDatum> dv) : NodeImplRecord(AVRO_RECORD, name, fields, fieldsNames, NoSize()),
                                                       defaultValues(std::move(dv)) {
    for (size_t i = 0; i < leafNameAttributes_.size(); ++i) {
        if (!nameIndex_.add(leafNameAttributes_.get(i), i)) {
            throw duckdb::InvalidInputException("Cannot add duplicate field: %s", leafNameAttributes_.get(i));
        }
    }
}

NodeMap::NodeMap() : NodeImplMap(AVRO_MAP) {
    NodePtr key(new NodePrimitive(AVRO_STRING));
    doAddLeaf(key);
}

} // namespace avro
