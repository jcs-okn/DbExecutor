using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Codeplex.Data.Options {
    internal class NullDbExecutorLogger : IDbExecutorLogger {

        public void PrepareExecute(string query, IDataParameterCollection parameters)
        {
        }

        public void SqlException(Exception ex)
        {
        }

        public void SqlException(string query, IDataParameterCollection parameters, Exception ex)
        {
        }
    }
}
