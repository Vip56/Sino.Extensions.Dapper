using System.Linq.Expressions;

namespace Sino.Extensions.Dapper.Expressions
{
    internal class UnaryExpressionToSql : BaseExpressionToSql<UnaryExpression>
    {
        protected override SqlBuilder Select(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Select(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Where(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Where(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder GroupBy(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.GroupBy(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder OrderBy(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.OrderBy(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Max(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Max(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Min(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Min(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Avg(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Avg(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Count(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Count(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }

        protected override SqlBuilder Sum(UnaryExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Sum(expression.Operand, sqlBuilder);
            return sqlBuilder;
        }
    }
}
