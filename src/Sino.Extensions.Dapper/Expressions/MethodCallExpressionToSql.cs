using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Sino.Extensions.Dapper.Expressions
{
    internal class MethodCallExpressionToSql : BaseExpressionToSql<MethodCallExpression>
    {
        static Dictionary<string, Action<MethodCallExpression, SqlBuilder>> _Methods = new Dictionary<string, Action<MethodCallExpression, SqlBuilder>>
        {
            {"Like",Like},
            {"LikeLeft",LikeLeft},
            {"LikeRight",LikeRight},
            {"In",InnerIn}
        };

        private static void InnerIn(MethodCallExpression expression, SqlBuilder sqlBuilder)
        {
            ExpressionToSqlProvider.Where(expression.Arguments[0], sqlBuilder);
            sqlBuilder += " in";
            ExpressionToSqlProvider.In(expression.Arguments[1], sqlBuilder);
        }

        private static void Like(MethodCallExpression expression, SqlBuilder sqlBuilder)
        {
            if (expression.Object != null)
            {
                ExpressionToSqlProvider.Where(expression.Object, sqlBuilder);
            }
            ExpressionToSqlProvider.Where(expression.Arguments[0], sqlBuilder);
            sqlBuilder += " like concat('%',";
            ExpressionToSqlProvider.Where(expression.Arguments[1], sqlBuilder);
            sqlBuilder += ",'%') ";
        }

        private static void LikeLeft(MethodCallExpression expression, SqlBuilder sqlBuilder)
        {
            if (expression.Object != null)
            {
                ExpressionToSqlProvider.Where(expression.Object, sqlBuilder);
            }
            ExpressionToSqlProvider.Where(expression.Arguments[0], sqlBuilder);
            sqlBuilder += " like concat('%',";
            ExpressionToSqlProvider.Where(expression.Arguments[1], sqlBuilder);
            sqlBuilder += ") ";
        }

        private static void LikeRight(MethodCallExpression expression, SqlBuilder sqlBuilder)
        {
            if (expression.Object != null)
            {
                ExpressionToSqlProvider.Where(expression.Object, sqlBuilder);
            }
            ExpressionToSqlProvider.Where(expression.Arguments[0], sqlBuilder);
            sqlBuilder += " like concat(";
            ExpressionToSqlProvider.Where(expression.Arguments[1], sqlBuilder);
            sqlBuilder += ",'%') ";
        }

        protected override SqlBuilder Where(MethodCallExpression expression, SqlBuilder sqlBuilder)
        {
            var key = expression.Method;
            if (key.IsGenericMethod)
            {
                key = key.GetGenericMethodDefinition();
            }

            Action<MethodCallExpression, SqlBuilder> action;
            if (_Methods.TryGetValue(key.Name, out action))
            {
                action(expression, sqlBuilder);
                return sqlBuilder;
            }

            throw new NotImplementedException("Unimplemented method:" + expression.Method);
        }
    }
}