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

#include "duckdb/common/exception.hpp"

#include "LogicalType.hh"

namespace avro {

LogicalType::LogicalType(Type type)
    : type_(type), precision_(0), scale_(0) {}

LogicalType::Type LogicalType::type() const {
    return type_;
}

void LogicalType::setPrecision(int precision) {
    if (type_ != DECIMAL) {
        throw duckdb::InvalidInputException("Only logical type DECIMAL can have precision");
    }
    if (precision <= 0) {
        throw duckdb::InvalidInputException("Precision cannot be: %d", precision);
    }
    precision_ = precision;
}

void LogicalType::setScale(int scale) {
    if (type_ != DECIMAL) {
        throw duckdb::InvalidInputException("Only logical type DECIMAL can have scale");
    }
    if (scale < 0) {
        throw duckdb::InvalidInputException("Scale cannot be: %d", scale);
    }
    scale_ = scale;
}

} // namespace avro
