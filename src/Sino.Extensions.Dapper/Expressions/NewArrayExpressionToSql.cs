using System.Linq.Expressions;

namespace Sino.Extensions.Dapper.Expressions
{
    internal class NewArrayExpressionToSql : BaseExpressionToSql<NewArrayExpression>
    {
        protected override SqlBuilder In(NewArrayExpression expression, SqlBuilder sqlBuilder)
        {
            sqlBuilder += "(";

            foreach (Expression expressionItem in expression.Expressions)
            {
                ExpressionToSqlProvider.In(expressionItem, sqlBuilder);
            }

            if (sqlBuilder[sqlBuilder.Length - 1] == ',')
            {
                sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            }

            sqlBuilder += ")";

            return sqlBuilder;
        }
    }
}
