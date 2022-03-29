#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#endif

#include "variant_value.hpp"
#include "variant-extension.hpp"

namespace duckdb {

namespace {

static void VariantFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR ? VectorType::CONSTANT_VECTOR
	                                                                                 : VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < args.size(); ++i) {
		result.SetValue(i, Variant(args.GetValue(0, i)));
	}
}

static void FromVariantFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	bool is_constant = true;
	for (idx_t i = 1; i < args.ColumnCount(); ++i) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			is_constant = false;
			break;
		}
	}
	result.SetVectorType(is_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	auto &type = result.GetType();
	for (idx_t i_row = 0; i_row < args.size(); ++i_row) {
		Value val = FromVariant(args.GetValue(1, i_row));
		const Value *vp = &val;
		for (idx_t i_idx = 2; i_idx < args.ColumnCount(); ++i_idx) {
			if (vp->IsNull()) {
				goto set_null;
			}
			switch (vp->type().id()) {
			case LogicalTypeId::STRUCT: {
				if (args.data[i_idx].GetType().id() != LogicalTypeId::VARCHAR) {
					goto set_null;
				}
				Value val_idx = args.GetValue(i_idx, i_row);
				if (val_idx.IsNull()) {
					goto set_null;
				}
				auto &child_types = StructType::GetChildTypes(vp->type());
				for (idx_t i = 0;; ++i) {
					if (i >= child_types.size()) {
						goto set_null;
					}
					if (child_types[i].first == StringValue::Get(val_idx)) {
						vp = &StructValue::GetChildren(*vp)[i];
						break;
					}
				}
				break;
			}
			case LogicalTypeId::LIST: {
				if (args.data[i_idx].GetType().id() == LogicalTypeId::VARCHAR) {
					goto set_null;
				}
				Value val_idx = args.GetValue(i_idx, i_row);
				if (val_idx.IsNull()) {
					goto set_null;
				}
				variant_index_type idx = val_idx.GetValue<variant_index_type>();
				if (idx >= ListValue::GetChildren(*vp).size()) {
					goto set_null;
				}
				vp = &ListValue::GetChildren(*vp)[idx];
				break;
			}
			default:
				goto set_null;
			}
		}
		if (vp->IsNull()) {
			goto set_null;
		}
		if (vp->type() != type) {
			auto cost = CastRules::ImplicitCast(vp->type(), type);
			if (cost < 0 || cost > 120 || !const_cast<Value *>(vp)->TryCastAs(type)) {
				goto set_null;
			}
		}
		result.SetValue(i_row, *vp);
		continue;
	set_null:
		if (is_constant) {
			ConstantVector::SetNull(result, true);
		} else {
			FlatVector::SetNull(result, i_row, true);
		}
	}
}

unique_ptr<FunctionData> FromVariantBind(ClientContext &context, ScalarFunction &bound_function,
                                         vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("type must be a constant");
	}
	Value type_str = ExpressionExecutor::EvaluateScalar(*arguments[0]);
	if (type_str.IsNull() || type_str.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("invalid type");
	}
	for (idx_t i = 2; i < arguments.size(); ++i) {
		const auto &type = arguments[i]->return_type;
		if (type.id() != LogicalTypeId::VARCHAR && !type.IsIntegral()) {
			throw InvalidInputException("indices must be of string or integer type");
		}
	}
	LogicalTypeId return_type_id = TransformStringToLogicalTypeId(StringValue::Get(type_str));
	if (return_type_id == LogicalTypeId::USER) {
		bound_function.return_type = TransformStringToLogicalType(StringValue::Get(type_str));
	} else {
		bound_function.return_type = return_type_id;
	}
	return nullptr;
}

} // namespace

void VariantExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	CreateScalarFunctionInfo variant_info(
	    ScalarFunction("variant", {LogicalType::ANY}, LogicalType::BLOB, VariantFunction));
	catalog.CreateFunction(context, &variant_info);

	CreateScalarFunctionInfo from_variant_info(
	    ScalarFunction("from_variant", {LogicalType::VARCHAR, LogicalType::BLOB}, LogicalType::ANY, FromVariantFunction,
	                   false, FromVariantBind, nullptr, nullptr, nullptr, LogicalType::ANY));
	catalog.CreateFunction(context, &from_variant_info);

	con.Commit();
}

string VariantExtension::Name() {
	return "variant";
}

} // namespace duckdb
