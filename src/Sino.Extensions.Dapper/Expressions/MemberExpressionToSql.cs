using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

namespace Sino.Extensions.Dapper.Expressions
{
    internal class MemberExpressionToSql : BaseExpressionToSql<MemberExpression>
    {
        private string GetTableName(string s)
        {
            string name = ExpressionHelper.GetTableName(s);
            return name;
        }

        private static object GetValue(MemberExpression expr)
        {
            object value;
            var field = expr.Member as FieldInfo;
            if (field != null)
            {
                value = field.GetValue(((ConstantExpression)expr.Expression).Value);
            }
            else
            {
                value = ((PropertyInfo)expr.Member).GetValue(((ConstantExpression)expr.Expression).Value, null);
            }
            return value;
        }

        private SqlBuilder AggregateFunctionParser(MemberExpression expression, SqlBuilder sqlBuilder,string operation)
        {
            string tableName = GetTableName(expression.Expression.Type.Name);
            string columnName = expression.Member.Name;

            sqlBuilder.SetTableAlias(tableName);
            string tableAlias = sqlBuilder.GetTableAlias(tableName);

            if (!string.IsNullOrWhiteSpace(tableAlias))
            {
                tableName += " " + tableAlias;
                columnName = tableAlias + "." + columnName;
            }
            sqlBuilder.AppendFormat("select {0}({1}) from {2}", operation, columnName, tableName);
            return sqlBuilder;
        }

        protected override SqlBuilder Select(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            sqlBuilder.SetTableAlias(GetTableName(expression.Expression.Type.Name));
            string tableAlias = sqlBuilder.GetTableAlias(GetTableName(expression.Expression.Type.Name));
            if (!string.IsNullOrWhiteSpace(tableAlias))
            {
                tableAlias += ".";
            }
            sqlBuilder.SelectFields.Add(tableAlias + expression.Member.Name);
            return sqlBuilder;
        }

        protected override SqlBuilder Join(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            sqlBuilder.SetTableAlias(GetTableName(expression.Expression.Type.Name));
            string tableAlias = sqlBuilder.GetTableAlias(GetTableName(expression.Expression.Type.Name));
            if (!string.IsNullOrWhiteSpace(tableAlias))
            {
                tableAlias += ".";
            }
            sqlBuilder += " " + tableAlias + expression.Member.Name;

            return sqlBuilder;
        }

        protected override SqlBuilder Where(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            if (expression.Expression.NodeType == ExpressionType.Constant)
            {
                object value = GetValue(expression);
                sqlBuilder.AddDbParameter(value);
            }
            else if (expression.Expression.NodeType == ExpressionType.Parameter)
            {
                sqlBuilder.SetTableAlias(GetTableName(expression.Expression.Type.Name));
                string tableAlias = sqlBuilder.GetTableAlias(GetTableName(expression.Expression.Type.Name));
                if (!string.IsNullOrWhiteSpace(tableAlias))
                {
                    tableAlias += ".";
                }
                sqlBuilder += " " + tableAlias + expression.Member.Name;
            }

            return sqlBuilder;
        }

        protected override SqlBuilder In(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            var field = expression.Member as FieldInfo;
            if (field != null)
            {
                object val = field.GetValue(((ConstantExpression)expression.Expression).Value);

                if (val != null)
                {
                    string itemJoinStr = "";
                    IEnumerable array = val as IEnumerable;
                    foreach (var item in array)
                    {
                        if (field.FieldType.Name == "String[]")
                        {
                            itemJoinStr += string.Format(",'{0}'", item);
                        }
						else if (field.FieldType.GetTypeInfo().IsEnum)
						{
							itemJoinStr += string.Format(",{0}", (int)item);
						}
						else
                        {
                            itemJoinStr += string.Format(",{0}", item);
                        }
                    }

                    if (itemJoinStr.Length > 0)
                    {
                        itemJoinStr = itemJoinStr.Remove(0, 1);
                        itemJoinStr = string.Format("({0})", itemJoinStr);
                        sqlBuilder += itemJoinStr;
                    }
                }
            }

            return sqlBuilder;
        }

        protected override SqlBuilder GroupBy(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            sqlBuilder.SetTableAlias(GetTableName(expression.Expression.Type.Name));
            sqlBuilder += sqlBuilder.GetTableAlias(GetTableName(expression.Expression.Type.Name)) + "." + expression.Member.Name;
            return sqlBuilder;
        }

        protected override SqlBuilder OrderBy(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            sqlBuilder.SetTableAlias(GetTableName(expression.Expression.Type.Name));
            sqlBuilder += sqlBuilder.GetTableAlias(GetTableName(expression.Expression.Type.Name)) + "." + expression.Member.Name;
            return sqlBuilder;
        }

        protected override SqlBuilder Max(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            return AggregateFunctionParser(expression, sqlBuilder,"max");
        }

        protected override SqlBuilder Min(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            return AggregateFunctionParser(expression, sqlBuilder,"min");
        }

        protected override SqlBuilder Avg(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            return AggregateFunctionParser(expression, sqlBuilder,"avg");
        }

        protected override SqlBuilder Count(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            return AggregateFunctionParser(expression, sqlBuilder,"count");
        }

        protected override SqlBuilder Sum(MemberExpression expression, SqlBuilder sqlBuilder)
        {
            return AggregateFunctionParser(expression, sqlBuilder,"sum");
        }
    }
}
