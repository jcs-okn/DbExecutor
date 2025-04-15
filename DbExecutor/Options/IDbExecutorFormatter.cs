using Codeplex.Data.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Codeplex.Data.Options
{
    public interface IDbExecutorFormatter
    {

        object Format(string key, object input);

    }
}
