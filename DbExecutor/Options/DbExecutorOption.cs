using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Codeplex.Data.Options
{
    public class DbExecutorOption
    {
        public DbExecutorOption()
        {
            this.ParameterSymbol = '@';
            this.IsUseTransaction = false;
            this.ExecuteReaderFormatter = null;
        }


        public char ParameterSymbol { get; set; }

        public bool IsUseTransaction { get; set; }

        public IsolationLevel IsolationLevel { get; set; }

        public IDbExecutorFormatter ExecuteReaderFormatter { get; set; } = null;


    }
}
