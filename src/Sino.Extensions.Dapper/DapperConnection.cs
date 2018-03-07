using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Dapper;

namespace Sino.Extensions.Dapper
{
    public class DapperConnection : IDapperConnection
    {
        protected bool IsUseRead { get; set; }
        protected bool IsUseWrite { get; set; }

        private IDbConnection _writeConnection;
        protected IDbConnection WriteConnection
        {
            get
            {
                IsUseWrite = true;
                return _writeConnection;
            }
            set
            {
                _writeConnection = value;
            }
        }

        private IDbConnection _readConnection;
        protected IDbConnection ReadConnection
        {
            get
            {
                IsUseRead = true;
                return _readConnection;
            }
            set
            {
                _readConnection = value;
            }
        }

        public DapperConnection(IDbConnection write, IDbConnection read)
        {
            if (write == null)
                throw new ArgumentNullException(nameof(write));
            if (read == null)
                throw new ArgumentNullException(nameof(read));

            ReadConnection = read;
            WriteConnection = write;
        }

        public IDbTransaction BeginTransaction()
        {
            return WriteConnection.BeginTransaction();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            return WriteConnection.BeginTransaction(il);
        }

        public int Execute(CommandDefinition command)
        {
            return WriteConnection.Execute(command);
        }

        public int Execute(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.Execute(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<int> ExecuteAsync(CommandDefinition command)
        {
            return WriteConnection.ExecuteAsync(command);
        }

        public Task<int> ExecuteAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.ExecuteAsync(sql, param, transaction, commandTimeout, commandType);
        }

        public IDataReader ExecuteReader(CommandDefinition command, CommandBehavior commandBehavior)
        {
            return WriteConnection.ExecuteReader(command, commandBehavior);
        }

        public IDataReader ExecuteReader(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.ExecuteReader(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<IDataReader> ExecuteReaderAsync(CommandDefinition command)
        {
            return WriteConnection.ExecuteReaderAsync(command);
        }

        public Task<IDataReader> ExecuteReaderAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.ExecuteReaderAsync(sql,param,transaction,commandTimeout,commandType);
        }

        public object ExecuteScalar(CommandDefinition command)
        {
            return WriteConnection.ExecuteScalar(command);
        }

        public object ExecuteScalar(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.ExecuteScalar(sql, param, transaction, commandTimeout, commandType);
        }

        public T ExecuteScalar<T>(CommandDefinition command)
        {
            return WriteConnection.ExecuteScalar<T>(command);
        }

        public T ExecuteScalar<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.ExecuteScalar<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<object> ExecuteScalarAsync(CommandDefinition command)
        {
            return WriteConnection.ExecuteScalarAsync(command);
        }

        public Task<object> ExecuteScalarAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.ExecuteScalarAsync(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<T> ExecuteScalarAsync<T>(CommandDefinition command)
        {
            return WriteConnection.ExecuteScalarAsync<T>(command);
        }

        public Task<T> ExecuteScalarAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return WriteConnection.ExecuteScalarAsync<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public IEnumerable<dynamic> Query(string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query(sql, param, transaction, buffered, commandTimeout, commandType);
        }

        public IEnumerable<object> Query(Type type, string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query(type, sql, param, transaction, buffered, commandTimeout, commandType);
        }

        public IEnumerable<T> Query<T>(CommandDefinition command)
        {
            return ReadConnection.Query<T>(command);
        }

        public IEnumerable<T> Query<T>(string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<T>(sql, param, transaction, buffered, commandTimeout, commandType);
        }

        public IEnumerable<TReturn> Query<TReturn>(string sql, Type[] types, Func<object[], TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), 
            CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<TReturn>(sql, types, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", 
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<TFirst, TSecond, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, 
            string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<TFirst, TSecond, TThird, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, IDbTransaction transaction = null, 
            bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<TFirst, TSecond, TThird, TFourth, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, IDbTransaction transaction = null, 
            bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, IDbTransaction transaction = null,
            bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, 
            IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.Query<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public Task<IEnumerable<dynamic>> QueryAsync(CommandDefinition command)
        {
            return ReadConnection.QueryAsync(command);
        }

        public Task<IEnumerable<object>> QueryAsync(Type type, CommandDefinition command)
        {
            return ReadConnection.QueryAsync(type, command);
        }

        public Task<IEnumerable<dynamic>> QueryAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<IEnumerable<object>> QueryAsync(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync(type, sql, param, transaction, commandTimeout, commandType);
        }

        public Task<IEnumerable<T>> QueryAsync<T>(CommandDefinition command)
        {
            return ReadConnection.QueryAsync<T>(command);
        }

        public Task<IEnumerable<T>> QueryAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TReturn>(string sql, Type[] types, Func<object[], TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", 
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<TReturn>(sql, types, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(CommandDefinition command, Func<TFirst, TSecond, TReturn> map, string splitOn = "Id")
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TReturn>(command, map, splitOn);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", 
            int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(CommandDefinition command, Func<TFirst, TSecond, TThird, TReturn> map, string splitOn = "Id")
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TReturn>(command, map, splitOn);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, 
            string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, string splitOn = "Id")
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(command, map, splitOn);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, 
            string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, string splitOn = "Id")
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(command, map, splitOn);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, object param = null, IDbTransaction transaction = null, 
            bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, string splitOn = "Id")
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(command, map, splitOn);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, object param = null, IDbTransaction transaction = null,
            bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, 
            string splitOn = "Id")
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(command, map, splitOn);
        }

        public Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, object param = null, 
            IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType);
        }

        public dynamic QueryFirst(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirst(sql, param, transaction, commandTimeout, commandType);
        }

        public object QueryFirst(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirst(type, sql, param, transaction, commandTimeout, commandType);
        }

        public T QueryFirst<T>(CommandDefinition command)
        {
            return ReadConnection.QueryFirst<T>(command);
        }

        public T QueryFirst<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirst<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<dynamic> QueryFirstAsync(CommandDefinition command)
        {
            return ReadConnection.QueryFirstAsync(command);
        }

        public Task<object> QueryFirstAsync(Type type, CommandDefinition command)
        {
            return ReadConnection.QueryFirstAsync(type, command);
        }

        public Task<object> QueryFirstAsync(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirstAsync(type, sql, param, transaction, commandTimeout, commandType);
        }

        public Task<T> QueryFirstAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirstAsync<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public dynamic QueryFirstOrDefault(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirstOrDefault(sql, param, transaction, commandTimeout, commandType);
        }

        public object QueryFirstOrDefault(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirstOrDefault(type, sql, param, transaction, commandTimeout, commandType);
        }

        public T QueryFirstOrDefault<T>(CommandDefinition command)
        {
            return ReadConnection.QueryFirstOrDefault<T>(command);
        }

        public T QueryFirstOrDefault<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirstOrDefault<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<dynamic> QueryFirstOrDefaultAsync(CommandDefinition command)
        {
            return ReadConnection.QueryFirstOrDefaultAsync(command);
        }

        public Task<object> QueryFirstOrDefaultAsync(Type type, CommandDefinition command)
        {
            return ReadConnection.QueryFirstOrDefaultAsync(type, command);
        }

        public Task<object> QueryFirstOrDefaultAsync(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirstOrDefaultAsync(type, sql, param, transaction, commandTimeout, commandType);
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryFirstOrDefaultAsync<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public SqlMapper.GridReader QueryMultiple(CommandDefinition command)
        {
            return ReadConnection.QueryMultiple(command);
        }

        public SqlMapper.GridReader QueryMultiple(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryMultiple(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<SqlMapper.GridReader> QueryMultipleAsync(CommandDefinition command)
        {
            return ReadConnection.QueryMultipleAsync(command);
        }

        public Task<SqlMapper.GridReader> QueryMultipleAsync(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QueryMultipleAsync(sql, param, transaction, commandTimeout, commandType);
        }

        public dynamic QuerySingle(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingle(sql, param, transaction, commandTimeout, commandType);
        }

        public object QuerySingle(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingle(type, sql, param, transaction, commandTimeout, commandType);
        }

        public T QuerySingle<T>(CommandDefinition command)
        {
            return ReadConnection.QuerySingle<T>(command);
        }

        public T QuerySingle<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingle<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<dynamic> QuerySingleAsync(CommandDefinition command)
        {
            return ReadConnection.QuerySingleAsync(command);
        }

        public Task<object> QuerySingleAsync(Type type, CommandDefinition command)
        {
            return ReadConnection.QuerySingleAsync(type, command);
        }

        public Task<object> QuerySingleAsync(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingleAsync(type, sql, param, transaction, commandTimeout, commandType);
        }

        public Task<T> QuerySingleAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingleAsync<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public dynamic QuerySingleOrDefault(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingleOrDefault(sql, param, transaction, commandTimeout, commandType);
        }

        public object QuerySingleOrDefault(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingleOrDefault(type, sql, param, transaction, commandTimeout, commandType);
        }

        public T QuerySingleOrDefault<T>(CommandDefinition command)
        {
            return ReadConnection.QuerySingleOrDefault<T>(command);
        }

        public T QuerySingleOrDefault<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingleOrDefault<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public Task<dynamic> QuerySingleOrDefaultAsync(CommandDefinition command)
        {
            return ReadConnection.QuerySingleOrDefaultAsync(command);
        }

        public Task<object> QuerySingleOrDefaultAsync(Type type, CommandDefinition command)
        {
            return ReadConnection.QuerySingleOrDefaultAsync(type, command);
        }

        public Task<object> QuerySingleOrDefaultAsync(Type type, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingleOrDefaultAsync(type, sql, param, transaction, commandTimeout, commandType);
        }

        public Task<T> QuerySingleOrDefaultAsync<T>(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            return ReadConnection.QuerySingleOrDefaultAsync<T>(sql, param, transaction, commandTimeout, commandType);
        }

        public void Dispose()
        {
            if (IsUseRead)
                ReadConnection.Dispose();
            if (IsUseWrite)
                WriteConnection.Dispose();

            IsUseWrite = false;
            IsUseRead = false;
        }
    }
}
