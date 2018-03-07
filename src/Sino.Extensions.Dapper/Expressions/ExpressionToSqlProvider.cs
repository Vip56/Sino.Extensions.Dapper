using System;
using System.Linq.Expressions;

namespace Sino.Extensions.Dapper.Expressions
{
    public class ExpressionToSqlProvider
    {
        private static IExpressionToSql GetExpression2Sql(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Cannot be null");
            }

            if (expression is BinaryExpression)
            {
                return new BinaryExpressionToSql();
            }
            if (expression is BlockExpression)
            {
                throw new NotImplementedException("Unimplemented BlockExpression2Sql");
            }
            if (expression is ConditionalExpression)
            {
                throw new NotImplementedException("Unimplemented ConditionalExpression2Sql");
            }
            if (expression is ConstantExpression)
            {
                return new ConstantExpressionToSql();
            }
            if (expression is DebugInfoExpression)
            {
                throw new NotImplementedException("Unimplemented DebugInfoExpression2Sql");
            }
            if (expression is DefaultExpression)
            {
                throw new NotImplementedException("Unimplemented DefaultExpression2Sql");
            }
            if (expression is DynamicExpression)
            {
                throw new NotImplementedException("Unimplemented DynamicExpression2Sql");
            }
            if (expression is GotoExpression)
            {
                throw new NotImplementedException("Unimplemented GotoExpression2Sql");
            }
            if (expression is IndexExpression)
            {
                throw new NotImplementedException("Unimplemented IndexExpression2Sql");
            }
            if (expression is InvocationExpression)
            {
                throw new NotImplementedException("Unimplemented InvocationExpression2Sql");
            }
            if (expression is LabelExpression)
            {
                throw new NotImplementedException("Unimplemented LabelExpression2Sql");
            }
            if (expression is LambdaExpression)
            {
                throw new NotImplementedException("Unimplemented LambdaExpression2Sql");
            }
            if (expression is ListInitExpression)
            {
                throw new NotImplementedException("Unimplemented ListInitExpression2Sql");
            }
            if (expression is LoopExpression)
            {
                throw new NotImplementedException("Unimplemented LoopExpression2Sql");
            }
            if (expression is MemberExpression)
            {
                return new MemberExpressionToSql();
            }
            if (expression is MemberInitExpression)
            {
                throw new NotImplementedException("Unimplemented MemberInitExpression2Sql");
            }
            if (expression is MethodCallExpression)
            {
                return new MethodCallExpressionToSql();
            }
            if (expression is NewArrayExpression)
            {
                return new NewArrayExpressionToSql();
            }
            if (expression is NewExpression)
            {
                return new NewExpressionToSql();
            }
            if (expression is ParameterExpression)
            {
                return new ParameterExpressionToSql();
            }
            if (expression is RuntimeVariablesExpression)
            {
                throw new NotImplementedException("Unimplemented RuntimeVariablesExpression2Sql");
            }
            if (expression is SwitchExpression)
            {
                throw new NotImplementedException("Unimplemented SwitchExpression2Sql");
            }
            if (expression is TryExpression)
            {
                throw new NotImplementedException("Unimplemented TryExpression2Sql");
            }
            if (expression is TypeBinaryExpression)
            {
                throw new NotImplementedException("Unimplemented TypeBinaryExpression2Sql");
            }
            if (expression is UnaryExpression)
            {
                return new UnaryExpressionToSql();
            }

            throw new NotImplementedException("Unimplemented Expression2Sql");
        }

        public static void Insert(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Insert(expression, sqlBuilder);
        }

        public static void Update(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Update(expression, sqlBuilder);
        }

        public static void Select(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Select(expression, sqlBuilder);
        }

        public static void Join(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Join(expression, sqlBuilder);
        }

        public static void Where(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Where(expression, sqlBuilder);
        }

        public static void In(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).In(expression, sqlBuilder);
        }

        public static void GroupBy(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).GroupBy(expression, sqlBuilder);
        }

        public static void OrderBy(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).OrderBy(expression, sqlBuilder);
        }

        public static void Max(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Max(expression, sqlBuilder);
        }

        public static void Min(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Min(expression, sqlBuilder);
        }

        public static void Avg(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Avg(expression, sqlBuilder);
        }

        public static void Count(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Count(expression, sqlBuilder);
        }

        public static void Sum(Expression expression, SqlBuilder sqlBuilder)
        {
            GetExpression2Sql(expression).Sum(expression, sqlBuilder);
        }
    }
}
