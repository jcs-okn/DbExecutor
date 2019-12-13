using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using Codeplex.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DbExecutorTest
{
    // codefirst model

    public class SqliteMaster
    {
        public string name { get; set; }
        public int rootpage { get; set; }
        public string sql { get; set; }
        public string tbl_name { get; set; }
        public string type { get; set; }
    }

    public class Departments
    {
        public string dept_no { get; set; }
        public string dept_name { get; set; }
        public string dept_address { get; set; }
    }

    public class Employer
    {
        public int UserId { get; set; }
        public string UserName { get; set; }
        public DateTime InsDate { get; set; }
    }

    public class SQLConnectionFactory
    {
        public const string DB_NAME = "test.db";
        public string GetConnection()
        {
            var d = new System.Data.SQLite.SQLiteConnectionStringBuilder() { DataSource = DB_NAME };
            d.Add("mode", "memory");
            d.Add("cache", "shared");
            return d.ToString();
        }
    }

    [TestClass]
    public class AssemblyInitializer
    {
        [AssemblyInitialize]
        public static void Init(TestContext tc)
        {
            Contract.ContractFailed += (sender, e) =>
            {
                e.SetUnwind();
                Assert.Fail(e.FailureKind.ToString() + ":" + e.Message);
            };

            if (System.IO.File.Exists(SQLConnectionFactory.DB_NAME))
            {
                System.IO.File.Delete(SQLConnectionFactory.DB_NAME);
            }

            using (var exec = new DbExecutor(new SQLiteConnection(new SQLConnectionFactory().GetConnection())))
            {
                var sb = new StringBuilder();
                sb.AppendLine("CREATE TABLE employees (");
                sb.AppendLine("    emp_no      INT             NOT NULL,");
                sb.AppendLine("    birth_date  DATE            NOT NULL,");
                sb.AppendLine("    first_name  VARCHAR(14)     NOT NULL,");
                sb.AppendLine("    last_name   VARCHAR(16)     NOT NULL,");
                sb.AppendLine("    gender      VARCHAR(1)  NOT NULL,");
                sb.AppendLine("    hire_date   DATE            NOT NULL,");
                sb.AppendLine("    PRIMARY KEY (emp_no)");
                sb.AppendLine(");");

                exec.ExecuteNonQuery(sb.ToString());
                sb.Clear();
                sb.AppendLine("CREATE TABLE departments (");
                sb.AppendLine("    dept_no     CHAR(4)         NOT NULL,");
                sb.AppendLine("    dept_name   VARCHAR(40)     NOT NULL,");
                sb.AppendLine("    dept_address   VARCHAR(40)          ,");
                sb.AppendLine("    PRIMARY KEY (dept_no)");
                sb.AppendLine(");");

                exec.ExecuteNonQuery(sb.ToString());
                sb.Clear();
                sb.AppendLine("CREATE TABLE dept_emp (");
                sb.AppendLine("    emp_no      INT         NOT NULL,");
                sb.AppendLine("    dept_no     CHAR(4)     NOT NULL,");
                sb.AppendLine("    from_date   DATE        NOT NULL,");
                sb.AppendLine("    to_date     DATE        NOT NULL,");
                sb.AppendLine("    PRIMARY KEY (emp_no, dept_no)");
                sb.AppendLine(");");

                exec.ExecuteNonQuery(sb.ToString());
                sb.Clear();
                sb.AppendLine("CREATE TABLE dept_manager (");
                sb.AppendLine("   dept_no      CHAR(4)  NOT NULL,");
                sb.AppendLine("   emp_no       INT      NOT NULL,");
                sb.AppendLine("   from_date    DATE     NOT NULL,");
                sb.AppendLine("   to_date      DATE     NOT NULL,");
                sb.AppendLine("   PRIMARY KEY (emp_no, dept_no)");
                sb.AppendLine(");");
                
                exec.ExecuteNonQuery(sb.ToString());
                sb.Clear();
                sb.AppendLine("CREATE TABLE titles (");
                sb.AppendLine("    emp_no      INT          NOT NULL,");
                sb.AppendLine("    title       VARCHAR(50)  NOT NULL,");
                sb.AppendLine("    from_date   DATE         NOT NULL,");
                sb.AppendLine("    to_date     DATE,");
                sb.AppendLine("    PRIMARY KEY (emp_no, title, from_date)");
                sb.AppendLine(");");

                exec.ExecuteNonQuery(sb.ToString());
                sb.Clear();
                sb.AppendLine("CREATE TABLE salaries (");
                sb.AppendLine("    emp_no      INT    NOT NULL,");
                sb.AppendLine("    salary      INT    NOT NULL,");
                sb.AppendLine("    from_date   DATE   NOT NULL,");
                sb.AppendLine("    to_date     DATE   NOT NULL,");
                sb.AppendLine("    PRIMARY KEY (emp_no, from_date)");
                sb.AppendLine(");");
                exec.ExecuteNonQuery(sb.ToString());

                var masters = exec.SelectDynamic("select * from sqlite_master ").ToArray();

                foreach (var item in masters)
                {
                    Console.WriteLine(item.name);
                }

                exec.TransactionComplete();
            }
        }
    }
}
