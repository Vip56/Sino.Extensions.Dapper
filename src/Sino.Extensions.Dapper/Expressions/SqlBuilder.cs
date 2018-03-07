using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Sino.Extensions.Dapper.Expressions
{
    public class SqlBuilder
    {
        private static readonly List<string> S_listEnglishWords = new List<string> { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };

        private Dictionary<string, string> _dicTableName = new Dictionary<string, string>();
        private Queue<string> _queueEnglishWords = new Queue<string>(S_listEnglishWords);
        private StringBuilder _sqlBuilder;

        internal bool IsSingleTable { get; set; }

        internal List<string> SelectFields { get; set; }
        internal List<string> SelectFieldsAlias { get; set; }

        internal string SelectFieldsStr
        {
            get
            {
                string selectFieldsStr = null;
                if (this.SelectFieldsAlias.Count > 0)
                {
                    for (int i = 0; i < this.SelectFields.Count; i++)
                    {
                        string field = this.SelectFields[i];
                        string fieldAlias = this.SelectFieldsAlias[i];
                        if (field.Split('.')[1] == fieldAlias)
                        {
                            selectFieldsStr += "," + field;
                        }
                        else
                        {
                            selectFieldsStr += "," + field + " " + fieldAlias;
                        }
                    }

                    if (selectFieldsStr.Length > 0 && selectFieldsStr[0] == ',')
                    {
                        selectFieldsStr = selectFieldsStr.Remove(0, 1);
                    }
                }
                else
                {
                    selectFieldsStr = string.Join(",", this.SelectFields);
                }

                return selectFieldsStr;
            }
        }

        internal int Length
        {
            get
            {
                return this._sqlBuilder.Length;
            }
        }

        internal string Sql
        {
            get { return this.ToString(); }
        }

        internal Dictionary<string, object> DbParams { get; private set; }

        internal char this[int index]
        {
            get
            {
                return this._sqlBuilder[index];
            }
        }

        internal SqlBuilder()
        {
            DbParams = new Dictionary<string, object>();
            _sqlBuilder = new StringBuilder();
            SelectFields = new List<string>();
            SelectFieldsAlias = new List<string>();
        }

        public static SqlBuilder operator +(SqlBuilder sqlBuilder, string sql)
        {
            sqlBuilder._sqlBuilder.Append(sql);
            return sqlBuilder;
        }

        internal void Clear()
        {
            this.SelectFields.Clear();
            this._sqlBuilder.Clear();
            this.DbParams.Clear();
            this._dicTableName.Clear();
            this._queueEnglishWords = new Queue<string>(S_listEnglishWords);
        }

        internal string AddDbParameter(object dbParamValue, bool allowAutoAppend = true)
        {
            string dbParamName = "";
            if (dbParamValue == null || dbParamValue == DBNull.Value)
            {
                if (allowAutoAppend)
                {
                    this._sqlBuilder.Append(" null");
                }
            }
            else
            {
                dbParamName = "?param" + DbParams.Count;
                DbParams.Add(dbParamName, dbParamValue);
                if (allowAutoAppend)
                {
                    this._sqlBuilder.Append(" " + dbParamName);
                }
            }
            return dbParamName;
        }

        internal bool SetTableAlias(string tableName)
        {
            if (!this._dicTableName.Keys.Contains(tableName))
            {
                this._dicTableName.Add(tableName, this._queueEnglishWords.Dequeue());
                return true;
            }
            return false;
        }

        internal string GetTableAlias(string tableName)
        {
            if (!this.IsSingleTable && this._dicTableName.Keys.Contains(tableName))
            {
                return this._dicTableName[tableName];
            }
            return "";
        }

        public override string ToString()
        {
            return this._sqlBuilder.ToString();
        }

        #region StringBuilder
        internal void Insert(int index, string value)
        {
            this._sqlBuilder.Insert(index, value);
        }

        internal void AppendFormat(string format, object arg0)
        {
            this._sqlBuilder.AppendFormat(format, arg0);
        }

        internal void AppendFormat(string format, object arg0, object arg1)
        {
            this._sqlBuilder.AppendFormat(format, arg0, arg1);
        }

        internal void AppendFormat(string format, object arg0, object arg1, object arg2)
        {
            this._sqlBuilder.AppendFormat(format, arg0, arg1, arg2);
        }

        internal void Remove(int startIndex, int length)
        {
            this._sqlBuilder.Remove(startIndex, length);
        }
        #endregion

    }
}
