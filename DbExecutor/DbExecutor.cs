using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Codeplex.Data.Internal;
using Codeplex.Data.Options;

namespace Codeplex.Data
{
    /// <summary>Simple and Lightweight Database Executor.</summary>
    public partial class DbExecutor : IDisposable
    {
        readonly IDbConnection connection;
        readonly char parameterSymbol;
        // Transaction
        readonly bool isUseTransaction;
        readonly IsolationLevel isolationLevel;
        readonly DbExecutorOption option;

        IDbTransaction transaction;
        bool isTransactionCompleted = false;
        public IDbExecutorLogger Logger;

        /// <summary>Create standard executor.</summary>
        /// <param name="connection">Database connection.</param>
        /// <param name="parameterSymbol">Command parameter symbol. SqlServer = '@', MySql = '?', Oracle = ':'</param>
        public DbExecutor(IDbConnection connection, char parameterSymbol = '@')
        {
            this.connection = connection;
            this.parameterSymbol = parameterSymbol;
            this.isUseTransaction = false;
            this.Logger = new NullDbExecutorLogger();
            this.option = null;
        }

        /// <summary>Use transaction.</summary>
        /// <param name="connection">Database connection.</param>
        /// <param name="isolationLevel">Transaction IsolationLevel.</param>
        /// <param name="parameterSymbol">Command parameter symbol. SqlServer = '@', MySql = '?', Oracle = ':'</param>
        public DbExecutor(IDbConnection connection, IsolationLevel isolationLevel, char parameterSymbol = '@')
        {
            this.connection = connection;
            this.parameterSymbol = parameterSymbol;
            this.isUseTransaction = true;
            this.isolationLevel = isolationLevel;
            this.Logger = new NullDbExecutorLogger();
            this.option = null;
        }

        public DbExecutor(IDbConnection connection, DbExecutorOption option)
        {
            this.connection = connection;
            this.parameterSymbol = option.ParameterSymbol;
            this.isUseTransaction = option.IsUseTransaction;
            this.Logger = new NullDbExecutorLogger();
            this.option = option;

            if (this.isUseTransaction) this.isolationLevel = option.IsolationLevel;
        }

        /// <summary>If connection is not open then open and create command.</summary>
        /// <param name="query">SQL code.</param>
        /// <param name="commandType">Command Type.</param>
        /// <param name="parameter">PropertyName parameterized to PropertyName. if null then no use parameter.</param>
        /// <param name="extraParameter">CommandName set to __extra__PropertyName.</param>
        /// <returns>Setuped IDbCommand.</returns>
        protected IDbCommand PrepareExecute(string query, CommandType commandType, object parameter, object extraParameter = null)
        {
            if (connection.State != ConnectionState.Open) connection.Open();
            if (transaction == null && isUseTransaction) transaction = connection.BeginTransaction(isolationLevel);

            var command = connection.CreateCommand();
            command.CommandText = query;
            command.CommandType = commandType;

            if (parameter != null)
            {
                if (parameter.GetType() == typeof(ExpandoObject))
                {
                    foreach (var p in (ExpandoObject)parameter)
                    {
                        var param = command.CreateParameter();
                        param.ParameterName = p.Key;
                        param.Value = p.Value ?? DBNull.Value;
                        command.Parameters.Add(param);
                    }
                }
                else
                {
                    foreach (var p in AccessorCache.Lookup(parameter.GetType()))
                    {
                        if (!p.IsReadable) continue;

                        var param = command.CreateParameter();
                        param.ParameterName = p.Name;
                        param.Value = p.GetValueDirect(parameter) ?? DBNull.Value;
                        command.Parameters.Add(param);
                    }
                }

            }
            if (extraParameter != null)
            {
                if (extraParameter.GetType() == typeof(ExpandoObject))
                {
                    foreach (var p in (ExpandoObject)extraParameter)
                    {
                        var param = command.CreateParameter();
                        param.ParameterName = "__extra__" + p.Key;
                        param.Value = p.Value ?? DBNull.Value;
                        command.Parameters.Add(param);
                    }
                }
                else
                {
                    foreach (var p in AccessorCache.Lookup(extraParameter.GetType()))
                    {
                        if (!p.IsReadable) continue;

                        var param = command.CreateParameter();
                        param.ParameterName = "__extra__" + p.Name;
                        param.Value = p.GetValueDirect(extraParameter) ?? DBNull.Value;
                        command.Parameters.Add(param);
                    }
                }
            }


            if (transaction != null) command.Transaction = transaction;

            Logger.PrepareExecute(command.CommandText, command.Parameters);

            return command;
        }

        IEnumerable<IDataRecord> YieldReaderHelper(string query, object parameter, CommandType commandType, CommandBehavior commandBehavior)
        {
            using (var command = PrepareExecute(query, commandType, parameter))
            {
                IDataReader reader;
                try
                {
                    reader = command.ExecuteReader(commandBehavior);
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }
                
                using (reader)
                {

                    bool b = false;
                    var sw = new System.Diagnostics.Stopwatch();
                    try
                    {
                        sw.Start();
                        while (true)
                        {
                            try
                            {
                                b = reader.Read();
                            }
                            catch (Exception ex)
                            {
                                Logger.SqlException(query, command.Parameters, ex);
                                throw;
                            }
                            if (b == false)
                            {
                                break;
                            }

                            yield return reader;
                        }
                    }
                    finally
                    {
                        sw.Stop();
                        Logger.YieldReaderFinished(command.CommandText, command.Parameters, sw.ElapsedMilliseconds);
                    }
                }
                    
            }
        }

        /// <summary>Executes and returns the data records.</summary>
        /// <param name="query">SQL code.</param>
        /// <param name="parameter">PropertyName parameterized to PropertyName. if null then no use parameter.</param>
        /// <param name="commandType">Command Type.</param>
        /// <param name="commandBehavior">Command Behavior.</param>
        /// <returns>Query results.</returns>
        public IEnumerable<IDataRecord> ExecuteReader(string query, object parameter = null, CommandType commandType = CommandType.Text, CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            return YieldReaderHelper(query, parameter, commandType, commandBehavior);
        }

        IEnumerable<dynamic> YieldReaderDynamicHelper(string query, object parameter, CommandType commandType, CommandBehavior commandBehavior)
        {
            using (var command = PrepareExecute(query, commandType, parameter))
            {
                IDataReader reader;
                try
                {
                    reader = command.ExecuteReader(commandBehavior);
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }

                using (reader)
                {
                    DynamicDataRecord record = new DynamicDataRecord(reader); // reference same reader
                    bool b = false;
                    while (true)
                    {
                        try
                        {
                            b = reader.Read();
                        }
                        catch (Exception ex)
                        {
                            Logger.SqlException(query, command.Parameters, ex);
                            throw;
                        }
                        if (b == false)
                        {
                            break;
                        }

                        yield return record;
                    }
                }
            }
        }

        /// <summary>Executes and returns the data records enclosing DynamicDataRecord.</summary>
        /// <param name="query">SQL code.</param>
        /// <param name="parameter">PropertyName parameterized to PropertyName. if null then no use parameter.</param>
        /// <param name="commandType">Command Type.</param>
        /// <param name="commandBehavior">Command Behavior.</param>
        /// <returns>Query results. Result type is DynamicDataRecord.</returns>
        public IEnumerable<dynamic> ExecuteReaderDynamic(string query, object parameter = null, CommandType commandType = CommandType.Text, CommandBehavior commandBehavior = CommandBehavior.Default)
        {
            return YieldReaderDynamicHelper(query, parameter, commandType, commandBehavior);
        }

        private void AddCommandParameter(object parameter, IDbCommand command, ParameterDirection? direction = null)
        {
            if (parameter.GetType() == typeof(ExpandoObject))
            {
                foreach (var p in (ExpandoObject)parameter)
                {
                    var param = command.CreateParameter();
                    param.ParameterName = p.Key;
                    param.Value = p.Value ?? DBNull.Value;
                    if (direction != null)
                    {
                        param.Direction = direction.Value;
                    }
                    command.Parameters.Add(param);
                }
            }
            else
            {
                foreach (var p in AccessorCache.Lookup(parameter.GetType()))
                {
                    if (!p.IsReadable) continue;

                    var param = command.CreateParameter();
                    param.ParameterName = p.Name;
                    param.Value = p.GetValueDirect(parameter) ?? DBNull.Value;
                    if (direction != null)
                    {
                        param.Direction = direction.Value;
                    }
                    command.Parameters.Add(param);
                }
            }
        }

        /// <summary>Executes and returns the number of rows affected.</summary>
        /// <param name="query">SQL code.</param>
        /// <param name="parameter">PropertyName parameterized to PropertyName. if null then no use parameter.</param>
        /// <param name="commandType">Command Type.</param>
        /// <returns>Rows affected.</returns>
        public int ExecuteNonQuery(string query, object parameter = null, CommandType commandType = CommandType.Text)
        {
            using (var command = PrepareExecute(query, commandType, parameter))
            {
                try
                {
                    return command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }
            }
        }


        /// <summary>Executes procedure and returns paramter.</summary>
        /// <param name="query">Procedure name.</param>
        /// <param name="inputParameter">ParameterDirection is Input parameter.</param>
        /// <param name="outputParameter">ParameterDirection is Output parameter.</param>
        /// <param name="inOutParameter">ParameterDirection is InputOutput parameter.</param>
        /// <param name="returnParameter">ParameterDirection is ReturnValue parameter.</param>
        /// <returns>Rows affected.</returns>
        public IDictionary<string, object> ExecuteProcedure(string query, object inputParameter = null, object outputParameter = null, object inOutParameter = null, object returnParameter = null)
        {
            using (var command = PrepareExecute(query, CommandType.StoredProcedure, inputParameter))
            {
                if (outputParameter != null)
                {
                    AddCommandParameter(outputParameter, command, ParameterDirection.Output);
                }

                if (inOutParameter != null)
                {
                    AddCommandParameter(inOutParameter, command, ParameterDirection.InputOutput);
                }

                if (returnParameter != null)
                {
                    AddCommandParameter(returnParameter, command, ParameterDirection.ReturnValue);
                }

                try
                {
                    command.ExecuteNonQuery();

                    IDictionary<string, object> expando = new ExpandoObject();
                    command.Parameters.Cast<IDbDataParameter>()
                        .Where(d => d.Direction == ParameterDirection.Output || d.Direction == ParameterDirection.InputOutput || d.Direction == ParameterDirection.ReturnValue)
                        .ToList()
                        .ForEach(x =>
                        {
                            expando.Add(x.ParameterName, x.Value);
                        });

                    return expando;
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }
            }
        }

        public IDictionary<string, object> ExecuteProcedure(string query, Action<IDbCommand> addComandParameterAction)
        {
            using (var command = PrepareExecute(query, CommandType.StoredProcedure, null))
            {
                addComandParameterAction.Invoke(command);

                try
                {
                    command.ExecuteNonQuery();

                    IDictionary<string, object> expando = new ExpandoObject();
                    command.Parameters.Cast<IDbDataParameter>()
                        .Where(d => d.Direction == ParameterDirection.Output || d.Direction == ParameterDirection.InputOutput || d.Direction == ParameterDirection.ReturnValue)
                        .ToList()
                        .ForEach(x =>
                        {
                            expando.Add(x.ParameterName, x.Value);
                        });

                    return expando;
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }
            }
        }

        public async Task<int> ExecuteNonQueryAsync(string query, object parameter = null, CommandType commandType = CommandType.Text, CancellationToken token = default)
        {
            using (var command = PrepareExecute(query, commandType, parameter))
            {
                try
                {
                    return await ((System.Data.Common.DbCommand)command).ExecuteNonQueryAsync(token);
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }
            }
        }

        /// <summary>Executes and returns the first column, first row.</summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="query">SQL code.</param>
        /// <param name="parameter">PropertyName parameterized to PropertyName. if null then no use parameter.</param>
        /// <param name="commandType">Command Type.</param>
        /// <returns>Query results of first column, first row.</returns>
        public T ExecuteScalar<T>(string query, object parameter = null, CommandType commandType = CommandType.Text)
        {
            using (var command = PrepareExecute(query, commandType, parameter))
            {
                try
                {
                    return (T)command.ExecuteScalar();
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }
            }
        }

        /// <summary>Executes and mapping objects by ColumnName - PropertyName.</summary>
        /// <typeparam name="T">Mapping target Class.</typeparam>
        /// <param name="query">SQL code.</param>
        /// <param name="parameter">PropertyName parameterized to PropertyName. if null then no use parameter.</param>
        /// <param name="commandType">Command Type.</param>
        /// <returns>Mapped instances.</returns>
        public IEnumerable<T> Select<T>(string query, object parameter = null, CommandType commandType = CommandType.Text, IDbExecutorFormatter formatter = null) where T : new()
        {
            var accessors = AccessorCache.Lookup(typeof(T));
            return ExecuteReader(query, parameter, commandType, CommandBehavior.SequentialAccess)
                .Select(dr =>
                {
                    // if T is ValueType then can't set SetValue
                    // must be boxed
                    object result = new T();
                    for (int i = 0; i < dr.FieldCount; i++)
                    {
                        if (dr.IsDBNull(i)) continue;

                        var accessor = accessors[dr.GetName(i)];
                        if (accessor != null && accessor.IsWritable)
                        {
                            IDbExecutorFormatter fmt = (formatter != null) ? formatter : option?.ExecuteReaderFormatter;
                            if (fmt != null)
                            {
                                var value = fmt.Format(dr.GetName(i), dr[i]);
                                accessor.SetValueDirect(result, value);
                            }
                            else
                            {
                                accessor.SetValueDirect(result, dr[i]);
                            }
                        }
                    }
                    return (T)result;
                });
        }

        /// <summary>Executes and mapping objects to ExpandoObject. Object is dynamic accessable by ColumnName.</summary>
        /// <param name="query">SQL code.</param>
        /// <param name="parameter">PropertyName parameterized to PropertyName. if null then no use parameter.</param>
        /// <param name="commandType">Command Type.</param>
        /// <returns>Mapped results(dynamic type is ExpandoObject).</returns>
        public IEnumerable<dynamic> SelectDynamic(string query, object parameter = null, CommandType commandType = CommandType.Text, IDbExecutorFormatter formatter = null)
        {
            return ExecuteReader(query, parameter, commandType, CommandBehavior.SequentialAccess)
                .Select(dr =>
                {
                    IDictionary<string, object> expando = new ExpandoObject();
                    for (int i = 0; i < dr.FieldCount; i++)
                    {
                        var value = dr.IsDBNull(i) ? null : dr.GetValue(i);
                        if (formatter != null)
                        {
                            value = formatter.Format(dr.GetName(i), value);
                        }
                        else if (option?.ExecuteReaderFormatter != null)
                        {
                            value = option.ExecuteReaderFormatter.Format(dr.GetName(i), value);
                        }

                        expando.Add(dr.GetName(i), value);
                    }
                    return expando;
                });
        }

        /// <summary>Insert by object's PropertyName.</summary>
        /// <param name="tableName">Target database's table.</param>
        /// <param name="insertItem">Table's column name extracted from PropertyName.</param>
        /// <returns>Rows affected.</returns>
        public int Insert(string tableName, object insertItem)
        {
            string column = "";
            string data = "";
            if (insertItem.GetType() == typeof(ExpandoObject))
            {
                column = string.Join(",", ((System.Dynamic.ExpandoObject)insertItem).Select(p => p.Key));
                data = string.Join(",", ((System.Dynamic.ExpandoObject)insertItem).Select(p => parameterSymbol + p.Key));
            }
            else
            {
                var propNames = AccessorCache.Lookup(insertItem.GetType())
                    .Where(p => p.IsReadable)
                    .ToArray();

                column = string.Join(", ", propNames.Select(p => p.Name));
                data = string.Join(", ", propNames.Select(p => parameterSymbol + p.Name));
            }

            var query = string.Format("insert into {0} ({1}) values ({2})", tableName, column, data);

            return ExecuteNonQuery(query, insertItem);
        }

        public int InsertMultiple(string tableName, List<object> insertItems)
        {
            string column = "";
            foreach (var insertItem in insertItems)
            {
                if (insertItem.GetType() == typeof(ExpandoObject))
                {
                    column = string.Join(",", ((System.Dynamic.ExpandoObject)insertItem).Select(p => p.Key));
                }
                else
                {
                    var propNames = AccessorCache.Lookup(insertItem.GetType())
                        .Where(p => p.IsReadable)
                        .ToArray();

                    column = string.Join(", ", propNames.Select(p => p.Name));
                }
                break;
            }

            var items = new List<string>();
            dynamic exo = new System.Dynamic.ExpandoObject();
            foreach (var record in insertItems.Select((item, index) => new { item, index }))
            {
                var insertItem = record.item;
                if (insertItem.GetType() == typeof(ExpandoObject))
                {
                    var data = new List<string>();
                    foreach (var p in ((System.Dynamic.ExpandoObject)insertItem))
                    {
                        var key = p.Key + $"__{record.index}";
                        ((IDictionary<string, object>)exo).Add(key, p.Value);

                        data.Add(parameterSymbol + key);
                        
                    }

                    items.Add(string.Join(",", data));

                    if (insertItem.GetType() == typeof(ExpandoObject))
                    {
                        column = string.Join(",", ((System.Dynamic.ExpandoObject)insertItem).Select(p => p.Key));
                    }
                    else
                    {
                        var propNames = AccessorCache.Lookup(insertItem.GetType())
                            .Where(p => p.IsReadable)
                            .ToArray();

                        column = string.Join(", ", propNames.Select(p => p.Name));
                    }

                }
                else
                {
                    var data = new List<string>();
                    foreach (var p in AccessorCache.Lookup(insertItem.GetType()))
                    {
                        if (!p.IsReadable) continue;

                        var key = p.Name + $"__{record.index}";
                        ((IDictionary<string, object>)exo).Add(key, p.GetValueDirect(insertItem));

                        data.Add(parameterSymbol + key);
                    }
                    items.Add(string.Join(",", data));
                }
            }
            string query = "";

            if (connection.GetType().Name == "OracleConnection")
            {
                var x = items.Select(d => $"INTO {tableName} ({column}) VALUES ({d}) ");
                var val = string.Join(" ", x);
                query = $"INSERT ALL {val} SELECT 1 FROM DUAL";
            }
            else
            {
                var val = string.Join("),(", items);
                query = $"insert into {tableName} ({column}) values ({val})";
            }

            using (var command = PrepareExecute(query, CommandType.Text, exo))
            {
                try
                {
                    return command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Logger.SqlException(query, command.Parameters, ex);
                    throw;
                }
            }
        }

        public async Task<int> InsertAsync(string tableName, object insertItem, CancellationToken token)
        {
            string column = "";
            string data = "";
            if (insertItem.GetType() == typeof(ExpandoObject))
            {
                column = string.Join(",", ((System.Dynamic.ExpandoObject)insertItem).Select(p => p.Key));
                data = string.Join(",", ((System.Dynamic.ExpandoObject)insertItem).Select(p => parameterSymbol + p.Key));
            }
            else
            {
                var propNames = AccessorCache.Lookup(insertItem.GetType())
                    .Where(p => p.IsReadable)
                    .ToArray();

                column = string.Join(", ", propNames.Select(p => p.Name));
                data = string.Join(", ", propNames.Select(p => parameterSymbol + p.Name));
            }

            var query = string.Format("insert into {0} ({1}) values ({2})", tableName, column, data);

            return await ExecuteNonQueryAsync(query, insertItem, CommandType.Text, token);
        }

        /// <summary>Update by object's PropertyName.</summary>
        /// <param name="tableName">Target database's table.</param>
        /// <param name="updateItem">Table's column name extracted from PropertyName.</param>
        /// <param name="whereCondition">Where condition extracted from PropertyName.</param>
        /// <returns>Rows affected.</returns>
        public int Update(string tableName, object updateItem, object whereCondition)
        {
            string update = "";
            if (updateItem.GetType() == typeof(ExpandoObject))
            {
                update = string.Join(",", ((System.Dynamic.ExpandoObject)updateItem).Select(p => p.Key + " = " + parameterSymbol + p.Key));
            }
            else
            {
                update = string.Join(", ", AccessorCache.Lookup(updateItem.GetType())
                    .Where(p => p.IsReadable)
                    .Select(p => p.Name + " = " + parameterSymbol + p.Name));
            }

            string where = "";
            if (whereCondition.GetType() == typeof(ExpandoObject))
            {
                where = string.Join(" and ", ((System.Dynamic.ExpandoObject)whereCondition).Select(p => p.Key + " = " + parameterSymbol + "__extra__" + p.Key));
            }
            else
            {
                where = string.Join(" and ", AccessorCache.Lookup(whereCondition.GetType())
                .Select(p => p.Name + " = " + parameterSymbol + "__extra__" + p.Name));
            }

            var query = string.Format("update {0} set {1} where {2}", tableName, update, where);

            using (var command = PrepareExecute(query, CommandType.Text, updateItem, whereCondition))
            {
                try
                {
                    return command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Logger.SqlException(ex);
                    throw;
                }
            }
        }

        /// <summary>Delete by object's PropertyName.</summary>
        /// <param name="tableName">Target database's table.</param>
        /// <param name="whereCondition">Where condition extracted from PropertyName.</param>
        /// <returns>Rows affected.</returns>
        public int Delete(string tableName, object whereCondition)
        {
            string where = "";
            if (whereCondition.GetType() == typeof(ExpandoObject))
            {
                where = string.Join(" and ", ((System.Dynamic.ExpandoObject)whereCondition).Select(p => p.Key + " = " + parameterSymbol + p.Key));
            }
            else
            {
                where = string.Join(" and ", AccessorCache.Lookup(whereCondition.GetType())
                    .Select(p => p.Name + " = " + parameterSymbol + p.Name));
            }
                

            var query = string.Format("delete from {0} where {1}", tableName, where);

            return ExecuteNonQuery(query, whereCondition);
        }

        /// <summary>Commit transaction.</summary>
        public virtual void TransactionComplete()
        {
            if (transaction != null)
            {
                transaction.Commit();
                isTransactionCompleted = true;
            }
        }

        /// <summary>Dispose inner connection.</summary>
        public void Dispose()
        {
            try
            {
                if (transaction != null && !isTransactionCompleted)
                {
                    transaction.Rollback();
                    isTransactionCompleted = true;
                }
            }
            finally
            {
                connection.Dispose();
            }
        }
    }
}