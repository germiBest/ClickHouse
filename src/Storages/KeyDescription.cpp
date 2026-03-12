#include <Storages/KeyDescription.h>

#include <Functions/IFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/quoteString.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_KEY;
}

KeyDescription::KeyDescription(const KeyDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , sample_block(other.sample_block)
    , column_names(other.column_names)
    , reverse_flags(other.reverse_flags)
    , data_types(other.data_types)
    , additional_columns(other.additional_columns)
    , sort_order_id(other.sort_order_id)
{
    if (other.expression)
        expression = other.expression->clone();
}

KeyDescription & KeyDescription::operator=(const KeyDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    if (other.expression_list_ast)
        expression_list_ast = other.expression_list_ast->clone();
    else
        expression_list_ast.reset();

    if (other.expression)
        expression = other.expression->clone();
    else
        expression.reset();

    sample_block = other.sample_block;
    column_names = other.column_names;
    reverse_flags = other.reverse_flags;
    data_types = other.data_types;

    /// additional_columns is a constant property. It should never be lost.
    if (additional_columns && !other.additional_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong key assignment, losing additional_columns");

    additional_columns = other.additional_columns;
    sort_order_id = other.sort_order_id;
    return *this;
}

void KeyDescription::recalculateWithNewAST(
    const ASTPtr & new_ast,
    const ColumnsDescription & columns,
    const ContextPtr & context)
{
    *this = getKeyFromAST(new_ast, columns, context, additional_columns);
}

void KeyDescription::recalculateWithNewColumns(
    const ColumnsDescription & new_columns,
    const ContextPtr & context)
{
    *this = getKeyFromAST(definition_ast, new_columns, context, additional_columns);
}

bool KeyDescription::moduloToModuloLegacyRecursive(ASTPtr node_expr)
{
    if (!node_expr)
        return false;

    auto * function_expr = node_expr->as<ASTFunction>();
    bool modulo_in_ast = false;
    if (function_expr)
    {
        if (function_expr->name == "modulo")
        {
            function_expr->name = "moduloLegacy";
            modulo_in_ast = true;
        }
        if (function_expr->arguments)
        {
            auto children = function_expr->arguments->children;
            for (const auto & child : children)
                modulo_in_ast |= moduloToModuloLegacyRecursive(child);
        }
    }

    return modulo_in_ast;
}

KeyDescription KeyDescription::getKeyFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    const ContextPtr & context,
    const std::optional<NamesAndTypesList> & additional_columns)
{
    KeyDescription result;
    result.definition_ast = definition_ast;
    result.additional_columns = additional_columns;
    auto key_expression_list = extractKeyExpressionList(definition_ast);
    checkExpressionDoesntContainSubqueries(*key_expression_list);

    result.expression_list_ast = make_intrusive<ASTExpressionList>();
    for (const auto & child : key_expression_list->children)
    {
        auto real_key = child;
        if (auto * elem = child->as<ASTStorageOrderByElement>())
        {
            real_key = elem->children.front();
            result.reverse_flags.emplace_back(elem->direction < 0);
        }

        result.expression_list_ast->children.push_back(real_key);
        result.column_names.emplace_back(real_key->getColumnName());
    }

    if (result.additional_columns)
    {
        for (const auto & col : *result.additional_columns)
        {
            ASTPtr column_identifier = make_intrusive<ASTIdentifier>(col.name);
            result.column_names.emplace_back(column_identifier->getColumnName());
            result.expression_list_ast->children.push_back(column_identifier);

            if (!result.reverse_flags.empty())
                result.reverse_flags.emplace_back(false);
        }
    }

    if (!result.reverse_flags.empty() && result.reverse_flags.size() != result.expression_list_ast->children.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The size of reverse_flags ({}) does not match the size of KeyDescription {}",
            result.reverse_flags.size(), result.expression_list_ast->children.size());

    {
        auto expr = result.expression_list_ast->clone();
        auto all_columns = columns.get(GetColumnsOptions(GetColumnsOptions::Kind::AllPhysical).withSubcolumns());
        if (result.additional_columns)
        {
            for (const auto & col : *result.additional_columns)
                if (!columns.has(col.name))
                    all_columns.push_back(col);
        }
        auto syntax_result = TreeRewriter(context).analyze(expr, all_columns);
        /// In expression we also need to store source columns
        result.expression = ExpressionAnalyzer(expr, syntax_result, context).getActions(false);
        /// In sample block we use just key columns
        result.sample_block = ExpressionAnalyzer(expr, syntax_result, context).getActions(true)->getSampleBlock();
    }

    for (size_t i = 0; i < result.sample_block.columns(); ++i)
    {
        result.data_types.emplace_back(result.sample_block.getByPosition(i).type);
        if (!result.data_types.back()->isComparable())
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY,
                            "Column {} with type {} is not allowed in key expression, it's not comparable",
                            backQuote(result.sample_block.getByPosition(i).name), result.data_types.back()->getName());

        auto check = [&](const IDataType & type)
        {
            if (isDynamic(type) || isVariant(type) || isObject(type))
                throw Exception(
                    ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY,
                    "Column with type Variant/Dynamic/JSON is not allowed in key expression. Consider using a subcolumn with a specific data "
                    "type instead (for example 'column.Int64' or 'json.some.path.:Int64' if its a JSON path subcolumn) or casting this column to a specific data type");
        };

        check(*result.data_types.back());
        result.data_types.back()->forEachChild(check);
    }

    return result;
}

ASTPtr KeyDescription::getOriginalExpressionList() const
{
    if (!expression_list_ast || reverse_flags.empty())
        return expression_list_ast;

    auto expr_list = make_intrusive<ASTExpressionList>();
    size_t size = expression_list_ast->children.size();
    for (size_t i = 0; i < size; ++i)
    {
        auto column_ast = make_intrusive<ASTStorageOrderByElement>();
        column_ast->children.push_back(expression_list_ast->children[i]);
        column_ast->direction = (!reverse_flags.empty() && reverse_flags[i]) ? -1 : 1;
        expr_list->children.push_back(std::move(column_ast));
    }

    return expr_list;
}

KeyDescription KeyDescription::buildEmptyKey()
{
    KeyDescription result;
    result.expression_list_ast = make_intrusive<ASTExpressionList>();
    result.expression = std::make_shared<ExpressionActions>(ActionsDAG(), ExpressionActionsSettings{});
    return result;
}

KeyDescription KeyDescription::parse(const String & str, const ColumnsDescription & columns, const ContextPtr & context, bool allow_order)
{
    KeyDescription result;
    if (str.empty())
        return result;

    ParserStorageOrderByClause parser(allow_order);
    ASTPtr ast = parseQuery(parser, "(" + str + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    FunctionNameNormalizer::visit(ast.get());

    return getKeyFromAST(ast, columns, context);
}

}
