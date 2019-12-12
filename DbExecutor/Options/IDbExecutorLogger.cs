using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Codeplex.Data.Options {
    public interface IDbExecutorLogger {
        void PrepareExecute(string query, IDataParameterCollection parameters);
    }
}
