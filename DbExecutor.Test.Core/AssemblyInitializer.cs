using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace DbExecutorTestCore {

    [TestClass]
    public class AssemblyInitializer {

        [AssemblyInitialize]
        public static void Init(TestContext tc)
        {

            DbExecutorTest.AssemblyInitializer.Init(tc);

        }
    }
}
