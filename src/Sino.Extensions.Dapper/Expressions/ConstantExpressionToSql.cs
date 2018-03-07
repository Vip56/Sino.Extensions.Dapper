using System.Linq.Expressions;
using System.Reflection;

namespace Sino.Extensions.Dapper.Expressions
{
    internal class ConstantExpressionToSql : BaseExpressionToSql<ConstantExpression>
    {
        protected override SqlBuilder Where(ConstantExpression expression, SqlBuilder sqlBuilder)
        {
            sqlBuilder.AddDbParameter(expression.Value);
            return sqlBuilder;
        }

        protected override SqlBuilder In(ConstantExpression expression, SqlBuilder sqlBuilder)
        {
            if (expression.Type.Name == "String")
            {
                sqlBuilder.AppendFormat("'{0}',", expression.Value);
            }
			else if (expression.Type.GetTypeInfo().IsEnum)
			{
				sqlBuilder.AppendFormat("{0},", (int)expression.Value);
			}
            else
            {
                sqlBuilder.AppendFormat("{0},", expression.Value);
            }
            return sqlBuilder;
        }
	}
}
