using System;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using Codeplex.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DbExecutorTest
{
    [TestClass]
    public class DbExecutorTest
    {
        public TestContext TestContext { get; set; }
        private static Func<IDbConnection> connectionFactory;

        [ClassInitialize]
        public static void Setup(TestContext tc)
        {
            //var connStr = new CSharpStructure().Database.Connection.ConnectionString;
            var connStr = new SQLConnectionFactory().GetConnection();
            connectionFactory = () => new SQLiteConnection(connStr);
        }

        [TestMethod]
        public void ExecuteReader()
        {
            var rs = DbExecutor.ExecuteReader(connectionFactory(),
                    "select * from sqlite_master where type=@TypeId ", new { TypeId = "table" }).ToArray();
            rs.Length.Is(6);

            using (var exec = new DbExecutor(connectionFactory()))
            {
                //select name from sqlite_master where type='table';
                var ri = exec.ExecuteReader("select * from sqlite_master where type=@TypeId ", new { TypeId = "table" })
                    .Select(dr => 
                    {
                        var x = new SqliteMaster();
                        x.name = (string)dr["name"];
                        x.rootpage = (int)dr["rootpage"];
                        x.sql = (string)dr["sql"];
                        x.tbl_name = (string)dr["tbl_name"];
                        x.type = (string)dr["type"];
                        return x;
                    })
                    .ToArray();
                ri.Length.Is(6);
                ri.Select(x => x.name).Is("employees", "departments", "dept_emp", "dept_manager", "titles", "salaries");
            }
        }

        [TestMethod]
        public void ExecuteReaderDynamic()
        {
            var rs = DbExecutor.ExecuteReaderDynamic(connectionFactory(),
                    "select * from sqlite_master where type = @TypeId", new { TypeId = "table" })
                .Select(d =>
                {
                    var x = new SqliteMaster();
                    x.name = (string)d.name;
                    x.rootpage = (int)d.rootpage;
                    x.sql = (string)d.sql;
                    x.tbl_name = (string)d.tbl_name;
                    x.type = (string)d.type;
                    return x;
                })
                .ToArray();
            rs.Length.Is(6);
            rs.Select(x => x.name).Is("employees", "departments", "dept_emp", "dept_manager", "titles", "salaries");

            using (var exec = new DbExecutor(connectionFactory()))
            {
                var ri = exec.ExecuteReaderDynamic("select * from sqlite_master where type = @TypeId", new { TypeId = "table" })
                    .Select(d =>
                    {
                        var x = new SqliteMaster();
                        x.name = (string)d.name;
                        x.rootpage = (int)d.rootpage;
                        x.sql = (string)d.sql;
                        x.tbl_name = (string)d.tbl_name;
                        x.type = (string)d.type;
                        return x;
                    })
                    .ToArray();
                ri.Length.Is(6);
                rs.Select(x => x.name).Is("employees", "departments", "dept_emp", "dept_manager", "titles", "salaries");
            }
        }

        [TestMethod]
        public void ExecuteNonQuery()
        {
            using (var exec = new DbExecutor(connectionFactory(), IsolationLevel.ReadCommitted))
            {
                var affected = exec.ExecuteNonQuery(
                    "insert into Departments(dept_no, dept_name) values(@dept_no, @dept_name)",
                    new { dept_no = "d1", dept_name = "dept_name" });
                affected.Is(1);
                
                var f = exec.Select<Departments>("select * from Departments order by dept_no desc").First();
                f.Is(t => t.dept_name == "dept_name");

                // Transaction Uncommit
            }

            // Transaction Rollback test.
            var xs = DbExecutor.Select<Departments>(connectionFactory(), "select * from Departments where dept_no = 'd1'").ToArray();
            xs.Count().Is(0);
        }

        [TestMethod]
        public void ExecuteScalar()
        {
            using (var exec = new DbExecutor(connectionFactory()))
            {
                exec.ExecuteScalar<long>("select @TypeId", new { TypeId = 2 })
                    .Is(2);

                exec.ExecuteScalar<object>("select null").Is(DBNull.Value);
            }

            DbExecutor.ExecuteScalar<string>(connectionFactory(), "select date('now')")
                .Is(DateTime.Now.ToString("yyyy-MM-dd"));

            DbExecutor.ExecuteScalar<object>(connectionFactory(), "select null")
                .Is(DBNull.Value);
        }

        [TestMethod]
        public void Select()
        {
            using (var exec = new DbExecutor(connectionFactory()))
            {
                var r = exec.Select<SqliteMaster>("select * from sqlite_master where type = @TypeId", new { TypeId = "table" }).ToArray();
                r.Length.Is(6);
                r[0].Is(t => t.tbl_name == "employees" && t.name == t.tbl_name);
                r[1].Is(t => t.tbl_name == "departments" && t.name == t.tbl_name);
                r[2].Is(t => t.tbl_name == "dept_emp" && t.name == t.tbl_name);
                r[3].Is(t => t.tbl_name == "dept_manager" && t.name == t.tbl_name);
                r[4].Is(t => t.tbl_name == "titles" && t.name == t.tbl_name);
                r[5].Is(t => t.tbl_name == "salaries" && t.name == t.tbl_name);
            }

            var methods = DbExecutor.Select<SqliteMaster>(connectionFactory(), @"
                    select * from sqlite_master where type = @TypeId
                    ", new { TypeId = "table" })
                .ToArray();
            methods.Length.Is(6);
            methods.Select(x => x.name).Is("employees", "departments", "dept_emp", "dept_manager", "titles", "salaries");
        }

        [TestMethod]
        public void SelectDynamic()
        {
            using (var exec = new DbExecutor(connectionFactory()))
            {
                var r = exec.SelectDynamic("select * from sqlite_master where type = @TypeId", new { TypeId = "table" }).ToArray();
                r.Length.Is(6);
                r.Select(x => x.rootpage).Is(2, 4, 6, 8, 10, 12);
                r.Select(x => x.name).Is("employees", "departments", "dept_emp", "dept_manager", "titles", "salaries");
            }

            var methods = DbExecutor.SelectDynamic(connectionFactory(), @"
                    select * from sqlite_master where type = @TypeId
                    ", new { TypeId = "table" })
                .ToArray();
            methods.Length.Is(6);
            methods.Select(x => x.name).Is("employees", "departments", "dept_emp", "dept_manager", "titles", "salaries");
        }

        [TestMethod]
        public void Delete()
        {
            DbExecutor.Delete(connectionFactory(), "Departments", new { dept_no = "2" });
            DbExecutor.Insert(connectionFactory(), "Departments", new { dept_no = "2", dept_name = "name2" });
            using (var exec = new DbExecutor(connectionFactory(), IsolationLevel.ReadCommitted))
            {
                exec.Select<Departments>("select * from Departments")
                    .Any(x => x.dept_no == "2")
                    .Is(true);

                exec.Delete("Departments", new { dept_no = "2" });

                exec.Select<Departments>("select * from Departments")
                    .Any(x => x.dept_no == "2")
                    .Is(false);
            }
        }

        [TestMethod]
        public void Insert()
        {
            DbExecutor.Delete(connectionFactory(), "Departments", new { dept_no = "1" });
            DbExecutor.Delete(connectionFactory(), "Departments", new { dept_no = "2" });
            using (var exec = new DbExecutor(connectionFactory(), IsolationLevel.ReadCommitted))
            {
                exec.Insert("Departments", new { dept_no = "1", dept_name = "name1" });

                exec.Insert("Departments", new { dept_no = "001", dept_name = "null_test001", dept_address = (string)null });
                exec.TransactionComplete(); // Transaction Commit
            }
            DbExecutor.Insert(connectionFactory(), "Departments", new { dept_no = "2", dept_name = "name2" });

            DbExecutor.Select<Departments>(connectionFactory(),
                    "select * from Departments where dept_name like @dept_name", new { dept_name = "name%" })
                .Count()
                .Is(2);

            DbExecutor.Delete(connectionFactory(), "Departments", new { dept_name = "name1" });
            DbExecutor.Delete(connectionFactory(), "Departments", new { dept_name = "name2" });

            DbExecutor.Select<Departments>(connectionFactory(),
                 "select * from Departments where dept_name like @dept_name", new { dept_name = "name%" })
             .Count()
             .Is(0);
        }

        [TestMethod]
        public void Update()
        {
            DbExecutor.Delete(connectionFactory(), "Departments", new { dept_no = "1" });
            DbExecutor.Insert(connectionFactory(), "Departments", new { dept_no = "1", dept_name = "name1" });

            using (var exec = new DbExecutor(connectionFactory(), IsolationLevel.ReadCommitted))
            {
                exec.Select<Departments>("select * from Departments where dept_no = 1")
                    .First()
                    .Is(x => x.dept_name == "name1");

                exec.Update("Departments", new { dept_name = "UpdateName" }, new { dept_no = 1 });

                exec.Select<Departments>("select * from Departments where dept_no = 1")
                    .First()
                    .Is(x => x.dept_name == "UpdateName");
            }

            DbExecutor.Select<Departments>(connectionFactory(), "select * from Departments where dept_no = 1")
                .First()
                .Is(x => x.dept_name == "name1");

            DbExecutor.Update(connectionFactory(), "Departments", new { dept_name = "UpdateName" }, new { dept_no = 1 });

            DbExecutor.Select<Departments>(connectionFactory(), "select * from Departments where dept_no = 1")
                .First()
                .Is(x => x.dept_name == "UpdateName");

            DbExecutor.Update(connectionFactory(), "Departments", new { dept_name = "Int32" }, new { dept_no = 1 });
        }
    }
}
