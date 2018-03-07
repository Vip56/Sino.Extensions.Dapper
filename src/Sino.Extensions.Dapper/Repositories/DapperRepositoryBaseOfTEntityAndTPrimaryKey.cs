using Sino.Domain.Entities;
using Sino.Domain.Repositories;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;
using Dapper;
using System.Reflection;
using System.Text;
using System;
using System.Linq;
using Sino.Extensions.Dapper;
using Sino.Extensions.Dapper.Expressions;

namespace Sino.Dapper.Repositories
{
    public class DapperRepositoryBase<TEntity, TPrimaryKey> : AbpRepositoryBase<TEntity, TPrimaryKey>
        where TEntity : class, IEntity<TPrimaryKey>
    {
        protected IDapperConfiguration Configurationn { get; set; }

        protected IDbConnection WriteConnection { get; set; }

        protected IDbConnection ReadConnection { get; set; }

        protected IDapperConnection Connection { get; set; }

        public DapperRepositoryBase(IDapperConfiguration configuration)
        {
            Configurationn = configuration;
            WriteConnection = new MySqlConnection(Configurationn.WriteConnectionString);
            ReadConnection = new MySqlConnection(Configurationn.ReadConnectionString);
            Connection = new DapperConnection(WriteConnection, ReadConnection);
        }

        public DapperRepositoryBase(IDapperConfiguration configuration, bool IsOriginalTableName)
        {
            Configurationn = configuration;
            WriteConnection = new MySqlConnection(Configurationn.WriteConnectionString);
            ReadConnection = new MySqlConnection(Configurationn.ReadConnectionString);
            SqlMapperExtensions.IsOriginalTableName = IsOriginalTableName;
            Connection = new DapperConnection(WriteConnection, ReadConnection);
        }

        public override Task<TEntity> FirstOrDefaultAsync(TPrimaryKey id)
        {
            using (ReadConnection)
            {
                return ReadConnection.GetAsync<TEntity>(id);
            }
        }

        public override Task<IEnumerable<TEntity>> GetAllListAsync()
        {
            using (ReadConnection)
            {
                return ReadConnection.GetAllAsync<TEntity>();
            }
        }

        public override Task<TEntity> GetAsync(TPrimaryKey id)
        {
            using (ReadConnection)
            {
                return ReadConnection.GetAsync<TEntity>(id);
            }
        }

        public override async Task<TPrimaryKey> InsertAndGetIdAsync(TEntity entity)
        {
            using (WriteConnection)
            {
                await WriteConnection.InsertAsync(entity);
                return entity.Id;
            }
        }

        public override async Task<TEntity> InsertAsync(TEntity entity)
        {
            using (WriteConnection)
            {
                await WriteConnection.InsertAsync(entity);
                return entity;
            }
        }

        public override async Task<TEntity> UpdateAsync(TEntity entity)
        {
            using (WriteConnection)
            {
                var count = await WriteConnection.UpdateAsync(entity);
                if (count > 0)
                {
                    return entity;
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 根据查询条件查询总数
        /// </summary>
        /// <param name="query">查询对象</param>
        /// <returns></returns>
        public override async Task<int> CountAsync(IQueryObject<TEntity> query)
        {
            var parameters = new DynamicParameters();
            var count = ExpressionHelper.Count<TEntity>();
            foreach (var where in query.QueryExpression)
            {
                count.Where(where);
            }

            foreach (KeyValuePair<string, object> item in count.DbParams)
            {
                parameters.Add(item.Key, item.Value);
            }

            using (ReadConnection)
            {
                var Count = await ReadConnection.QuerySingleAsync<int>(count.Sql, parameters);
                return Count;
            }
        }

        /// <summary>
        /// 根据查询对象返回集合与总数
        /// </summary>
        /// <param name="query">查询对象</param>
        /// <returns></returns>
        public override async Task<Tuple<int, IList<TEntity>>> GetListAsync(IQueryObject<TEntity> query)
        {
            var parameters = new DynamicParameters();
            var select = ExpressionHelper.Select<TEntity>();
            var count = ExpressionHelper.Count<TEntity>();

            foreach (var where in query.QueryExpression)
            {
                select.Where(where);
            }
            foreach (var where in query.QueryExpression)
            {
                count.Where(where);
            }
            if (query.OrderSort == SortOrder.ASC)
            {
                select.OrderBy(query.OrderField);
            }
            else if (query.OrderSort == SortOrder.DESC)
            {
                select.OrderByDesc(query.OrderField);
            }
            if (query.Count >= 0)
            {
                select.Limit(query.Skip, query.Count);
            }

            foreach (KeyValuePair<string, object> item in select.DbParams)
            {
                parameters.Add(item.Key, item.Value);
            }

            using (ReadConnection)
            {
                var customerRepresentativeList = await ReadConnection.QueryAsync<TEntity>(select.Sql, parameters);
                int totalCount = await ReadConnection.QuerySingleAsync<int>(count.Sql, parameters);

                return new Tuple<int, IList<TEntity>>(totalCount, customerRepresentativeList.ToList());
            }
        }

        /// <summary>
        /// 组装插入语句
        /// </summary>
        /// <param name="tbName">数据库表名</param>
        protected string CreateInertSql<T>(string tbName)
        {
            var columns = new List<string>();
            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                var type = propertyInfo.PropertyType.GetTypeInfo();
                if (!type.IsInterface && (!type.IsClass || type.IsSealed))
                {
                    columns.Add(propertyInfo.Name);
                }
            }
            StringBuilder sql = new StringBuilder();
            sql.Append(string.Format("INSERT INTO {0}(", tbName));
            for (int i = 0; i < columns.Count; i++)
            {
                if (i == 0) sql.Append("`" + columns[i] + "`");
                else sql.Append(string.Format(",`{0}`", columns[i]));
            }
            sql.Append(") VALUES(");
            for (int i = 0; i < columns.Count; i++)
            {
                string comma = "";
                if (i != 0)
                {
                    comma = ",";
                }
                string columnSql = string.Format(comma + "@{0}", columns[i]);
                sql.Append(columnSql);
            }
            sql.Append(") ");
            return sql.ToString();
        }

        /// <summary>
        /// 生成动态参数
        /// </summary>
        /// <typeparam name="T">参数值类型</typeparam>
        /// <param name="t">参数值</param>
        protected DynamicParameters CreateParameters<T>(T t)
        {
            DynamicParameters param = new DynamicParameters();
            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                var type = propertyInfo.PropertyType.GetTypeInfo();
                if (!type.IsInterface && (!type.IsClass || type.IsSealed))
                {
                    param.Add("@" + propertyInfo.Name, propertyInfo.GetValue(t));
                }
            }

            return param;
        }

        /// <summary>
		/// 组装全更新语句
		/// </summary>
		/// <param name="tbName">表名</param>
        /// <param name="tbName">更新主键</param>
		/// <returns></returns>
        protected string UpdateSql<T>(string tbName, string keyName)
        {
            var columns = new List<string>();
            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                if (!propertyInfo.PropertyType.GetTypeInfo().IsInterface && (!propertyInfo.PropertyType.GetTypeInfo().IsClass || propertyInfo.PropertyType.GetTypeInfo().IsSealed))
                {
                    columns.Add(propertyInfo.Name);
                }
            }
            StringBuilder sql = new StringBuilder();
            sql.Append(string.Format("UPDATE {0} SET ", tbName));
            bool IsFirst = true;
            for (int i = 0; i < columns.Count; i++)
            {
                if (columns[i] != keyName)
                {
                    if (IsFirst)
                    {
                        sql.Append(string.Format("{0}=@{0}", columns[i]));
                        IsFirst = false;
                    }
                    else
                    {
                        sql.Append(string.Format(",{0}=@{0}", columns[i]));
                    }
                }
            }
            sql.Append(string.Format(" WHERE {0}=@{0};", keyName));
            return sql.ToString();
        }

        /// <summary>
        /// 根据参数组装更新语句
        /// </summary>
        /// <param name="tbName">表名</param>
        /// <param name="keyName">更新主键</param>
        /// <param name="param">参数</param>
        /// <returns></returns>
        protected string UpdateSql(string tbName, string keyName, DynamicParameters param)
        {
            StringBuilder sql = new StringBuilder();
            sql.Append(string.Format("UPDATE {0} SET ", tbName));
            for (int i = 0; i < param.ParameterNames.Count(); i++)
            {
                if (param.ParameterNames.ElementAt(i) != keyName)
                {
                    if (i == 0) sql.Append(string.Format("{0}=@{0}", param.ParameterNames.ElementAt(i)));
                    else sql.Append(string.Format(",{0}=@{0}", param.ParameterNames.ElementAt(i)));
                }
            }
            sql.Append(string.Format(" WHERE {0}=@{0};", keyName));
            return sql.ToString();
        }

        /// <summary>
        /// 根据所需参数名组装部分更新语句
        /// </summary>
        /// <param name="paramNames"></param>
        /// <returns></returns>
        protected string ParamsUpdateSql(List<string> paramNames)
        {
            StringBuilder sql = new StringBuilder();
            for (int i = 0; i < paramNames.Count(); i++)
            {
                if (i == 0) sql.Append(string.Format("{0}=@{0}", paramNames[i]));
                else sql.Append(string.Format(",{0}=@{0}", paramNames[i]));
            }
            return sql.ToString();
        }

        /// <summary>
        /// 根据所需参数名生成动态参数
        /// </summary>
        /// <typeparam name="T">参数值类型</typeparam>
        /// <param name="t">参数值</param>
        /// <param name="paramNames">所需参数名</param>
        /// <returns></returns>
        protected DynamicParameters CreateParameters<T>(T t, List<string> paramNames)
        {
            DynamicParameters param = new DynamicParameters();
            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                if (paramNames.Contains(propertyInfo.Name))
                {
                    param.Add("@" + propertyInfo.Name, propertyInfo.GetValue(t));
                }
            }

            return param;
        }
    }
}