using System.Linq.Expressions;
using System.Reflection;

namespace Sino.Extensions.Dapper.Expressions
{
    internal class NewExpressionToSql : BaseExpressionToSql<NewExpression>
    {
        protected override SqlBuilder Where(NewExpression expression, SqlBuilder sqlBuilder)
        {
            return base.Where(expression, sqlBuilder);
        }

        protected override SqlBuilder Insert(NewExpression expression, SqlBuilder sqlBuilder)
        {
            string columns = " (";
            string values = " values (";

            for (int i = 0; i < expression.Members.Count; i++)
            {
                MemberInfo m = expression.Members[i];
                columns += m.Name + ",";

                ConstantExpression c = expression.Arguments[i] as ConstantExpression;
                string dbParamName = sqlBuilder.AddDbParameter(c.Value, false);
                values += dbParamName + ",";
            }

            if (columns[columns.Length - 1] == ',')
            {
                columns = columns.Remove(columns.Length - 1, 1);
            }
            columns += ")";

            if (values[values.Length - 1] == ',')
            {
                values = values.Remove(values.Length - 1, 1);
            }
            values += ")";

            sqlBuilder += columns + values;

            return sqlBuilder;
        }

        protected override SqlBuilder Update(NewExpression expression, SqlBuilder sqlBuilder)
        {
            for (int i = 0; i < expression.Members.Count; i++)
            {
                MemberInfo m = expression.Members[i];
                ConstantExpression c = expression.Arguments[i] as ConstantExpression;
                sqlBuilder += m.Name + " =";
                sqlBuilder.AddDbParameter(c.Value);
                sqlBuilder += ",";
            }
            if (sqlBuilder[sqlBuilder.Length - 1] == ',')
            {
                sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            }
            return sqlBuilder;
        }

        protected override SqlBuilder Select(NewExpression expression, SqlBuilder sqlBuilder)
        {
            foreach (Expression item in expression.Arguments)
            {
                ExpressionToSqlProvider.Select(item, sqlBuilder);
            }

            foreach (MemberInfo item in expression.Members)
            {
                sqlBuilder.SelectFieldsAlias.Add(item.Name);
            }

            return sqlBuilder;
        }

        protected override SqlBuilder GroupBy(NewExpression expression, SqlBuilder sqlBuilder)
        {
            foreach (Expression item in expression.Arguments)
            {
                ExpressionToSqlProvider.GroupBy(item, sqlBuilder);
            }
            return sqlBuilder;
        }

        protected override SqlBuilder OrderBy(NewExpression expression, SqlBuilder sqlBuilder)
        {
            foreach (Expression item in expression.Arguments)
            {
                ExpressionToSqlProvider.OrderBy(item, sqlBuilder);
            }
            return sqlBuilder;
        }
    }
}
