using System.Linq.Expressions;

namespace Sino.Extensions.Dapper.Expressions
{
    internal class ParameterExpressionToSql : BaseExpressionToSql<ParameterExpression>
    {
        protected override SqlBuilder Select(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Select(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Where(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Where(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder GroupBy(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.GroupBy(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder OrderBy(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.OrderBy(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Max(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Max(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Min(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Min(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Avg(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Avg(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Count(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Count(expression, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Sum(ParameterExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Sum(expression, sqlBuilder);
            return sqlBuilder;
        }
    }
}
