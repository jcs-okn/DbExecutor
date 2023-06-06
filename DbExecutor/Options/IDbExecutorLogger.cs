using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Codeplex.Data.Options {
    public interface IDbExecutorLogger {
        void PrepareExecute(string query, IDataParameterCollection parameters);

        void SqlException(string query, IDataParameterCollection parameters, Exception ex);
        void SqlException(Exception ex);

        void YieldReaderFinished(string query, IDataParameterCollection parameters, double elapsed);
    }
}
