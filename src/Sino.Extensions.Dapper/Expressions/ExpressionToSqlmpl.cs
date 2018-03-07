using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Sino.Extensions.Dapper.Expressions
{
    public class ExpressionToSqlmpl<T>
    {
        private SqlBuilder _sqlBuilder;
        private string _mainTableName= typeof(T).Name;

        public string Sql
        {
            get
            {
                return _sqlBuilder.Sql + ";";
            }
        }

        public Dictionary<string, object> DbParams
        {
            get
            {
                return _sqlBuilder.DbParams;
            }
        }

        public ExpressionToSqlmpl()
        {
            _sqlBuilder = new SqlBuilder();
        }

        private string GetTableName(string s)
        {
            string name = "";
            var length = s.Length;
            var last = s.Substring(length - 1, 1);
            if (last == "y")
            {
                name = s.Substring(0, length - 1) + "ies";
            }
            else if (last == "o")
            {
                name = s + "es";
            }
            else if (last == "s")
            {
                name = s;
            }
            else
            {
                name = s + "s";
            }
            return name;
        }

        public void Clear()
        {
            _sqlBuilder.Clear();
        }


        private ExpressionToSqlmpl<T> SelectParser(Expression expression, Expression expressionBody, params Type[] ary)
        {
            this.Clear();
            this._sqlBuilder.IsSingleTable = false;

            if (expressionBody != null && expressionBody.Type == typeof(T))
            {
                throw new ArgumentException("cannot be parse expression", "expression");
            }

            foreach (var item in ary)
            {
                string tableName = item.Name;
                this._sqlBuilder.SetTableAlias(GetTableName(tableName));
            }

            string sql = "select {0}\nfrom " + GetTableName(this._mainTableName) + " " + this._sqlBuilder.GetTableAlias(GetTableName(this._mainTableName));

            if (expression == null)
            {
                _sqlBuilder.AppendFormat(sql, "*");
            }
            else
            {
                ExpressionToSqlProvider.Select(expressionBody, this._sqlBuilder);
                _sqlBuilder.AppendFormat(sql, this._sqlBuilder.SelectFieldsStr);
            }

            return this;
        }

        public ExpressionToSqlmpl<T> Select(Expression<Func<T, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2>(Expression<Func<T, T2, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3>(Expression<Func<T, T2, T3, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3, T4>(Expression<Func<T, T2, T3, T4, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3, T4, T5>(Expression<Func<T, T2, T3, T4, T5, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3, T4, T5, T6>(Expression<Func<T, T2, T3, T4, T5, T6, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3, T4, T5, T6, T7>(Expression<Func<T, T2, T3, T4, T5, T6, T7, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3, T4, T5, T6, T7, T8>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3, T4, T5, T6, T7, T8, T9>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        public ExpressionToSqlmpl<T> Select<T2, T3, T4, T5, T6, T7, T8, T9, T10>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, object>> expression = null)
        {
            return SelectParser(expression, expression == null ? null : expression.Body, typeof(T));
        }

        private ExpressionToSqlmpl<T> JoinParser<T2>(Expression<Func<T, T2, bool>> expression, string leftOrRightJoin = "")
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            string joinTableName = typeof(T2).Name;
            this._sqlBuilder.SetTableAlias(GetTableName(joinTableName));
            this._sqlBuilder.AppendFormat("\n{0}join {1} on", leftOrRightJoin, GetTableName(joinTableName) + " " + this._sqlBuilder.GetTableAlias(GetTableName(joinTableName)));
            ExpressionToSqlProvider.Join(expression.Body, this._sqlBuilder);
            return this;
        }

        private ExpressionToSqlmpl<T> JoinParser2<T2, T3>(Expression<Func<T2, T3, bool>> expression, string leftOrRightJoin = "")
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            string joinTableName = typeof(T3).Name;
            this._sqlBuilder.SetTableAlias(GetTableName(joinTableName));
            this._sqlBuilder.AppendFormat("\n{0}join {1} on", leftOrRightJoin, GetTableName(joinTableName) + " " + this._sqlBuilder.GetTableAlias(GetTableName(joinTableName)));
            ExpressionToSqlProvider.Join(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> Join<T2>(Expression<Func<T, T2, bool>> expression)
        {
            return JoinParser(expression);
        }

        public ExpressionToSqlmpl<T> Join<T2, T3>(Expression<Func<T2, T3, bool>> expression)
        {
            return JoinParser2(expression);
        }

        public ExpressionToSqlmpl<T> InnerJoin<T2>(Expression<Func<T, T2, bool>> expression)
        {
            return JoinParser(expression, "inner ");
        }

        public ExpressionToSqlmpl<T> InnerJoin<T2, T3>(Expression<Func<T2, T3, bool>> expression)
        {
            return JoinParser2(expression, "inner ");
        }

        public ExpressionToSqlmpl<T> LeftJoin<T2>(Expression<Func<T, T2, bool>> expression)
        {
            return JoinParser(expression, "left ");
        }

        public ExpressionToSqlmpl<T> LeftJoin<T2, T3>(Expression<Func<T2, T3, bool>> expression)
        {
            return JoinParser2(expression, "left ");
        }

        public ExpressionToSqlmpl<T> RightJoin<T2>(Expression<Func<T, T2, bool>> expression)
        {
            return JoinParser(expression, "right ");
        }

        public ExpressionToSqlmpl<T> RightJoin<T2, T3>(Expression<Func<T2, T3, bool>> expression)
        {
            return JoinParser2(expression, "right ");
        }

        public ExpressionToSqlmpl<T> FullJoin<T2>(Expression<Func<T, T2, bool>> expression)
        {
            return JoinParser(expression, "full ");
        }

        public ExpressionToSqlmpl<T> FullJoin<T2, T3>(Expression<Func<T2, T3, bool>> expression)
        {
            return JoinParser2(expression, "full ");
        }

        public ExpressionToSqlmpl<T> Where(Expression<Func<T, bool>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            if (expression.Body != null && expression.Body.NodeType == ExpressionType.Constant)
            {
                throw new ArgumentException("Cannot be parse expression", "expression");
            }

            if (this._sqlBuilder.Sql.Contains("where"))
            {
                this._sqlBuilder += "\n&&";
            }
            else
            {
                this._sqlBuilder += "\nwhere";
            }
            ExpressionToSqlProvider.Where(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> GroupBy(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            this._sqlBuilder += "\ngroup by ";
            ExpressionToSqlProvider.GroupBy(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> OrderBy(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            _sqlBuilder += "\norder by ";
            ExpressionToSqlProvider.OrderBy(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> OrderByDesc(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            _sqlBuilder += "\norder by ";
            ExpressionToSqlProvider.OrderBy(expression.Body, this._sqlBuilder);
            _sqlBuilder += " DESC";
            return this;
        }

        public ExpressionToSqlmpl<T> Max(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            Clear();
            ExpressionToSqlProvider.Max(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> Min(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            Clear();
            ExpressionToSqlProvider.Min(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> Avg(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            Clear();
            ExpressionToSqlProvider.Avg(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> Count(Expression<Func<T, object>> expression = null)
        {
            this.Clear();
            if (expression == null)
            {
                string tableName = GetTableName(typeof(T).Name);

                _sqlBuilder.SetTableAlias(tableName);
                string tableAlias = _sqlBuilder.GetTableAlias(tableName);

                if (!string.IsNullOrWhiteSpace(tableAlias))
                {
                    tableName += " " + tableAlias;
                }
                _sqlBuilder.AppendFormat("select count(*) from {0}", tableName);
            }
            else
            {
                ExpressionToSqlProvider.Count(expression.Body, this._sqlBuilder);
            }

            return this;
        }

        public ExpressionToSqlmpl<T> Sum(Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            Clear();
            ExpressionToSqlProvider.Sum(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> Insert(Expression<Func<object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            Clear();
            _sqlBuilder.IsSingleTable = true;
            _sqlBuilder.AppendFormat("insert into {0}", GetTableName(this._mainTableName));
            ExpressionToSqlProvider.Insert(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> Delete()
        {
            Clear();
            _sqlBuilder.IsSingleTable = true;
            _sqlBuilder.SetTableAlias(GetTableName(this._mainTableName));
            _sqlBuilder.AppendFormat("delete {0}", GetTableName(this._mainTableName));
            return this;
        }

        public ExpressionToSqlmpl<T> Update(Expression<Func<object>> expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression", "Value cannot be null");
            }

            Clear();
            _sqlBuilder.IsSingleTable = true;
            _sqlBuilder.AppendFormat("update {0} set ", GetTableName(this._mainTableName));
            ExpressionToSqlProvider.Update(expression.Body, this._sqlBuilder);
            return this;
        }

        public ExpressionToSqlmpl<T> Limit(int skip, int count)
        {
            _sqlBuilder += $"\nLIMIT {skip},{count}";
            return this;
        }
    }
}