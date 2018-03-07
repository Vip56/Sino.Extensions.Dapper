using Dapper;
using MySql.Data.MySqlClient;
using System.Linq;
using Xunit;

namespace Sino.Extensions.Dapper
{
    public class MapperTest
    {
        protected MySqlConnection Connection;

        public MapperTest()
        {
            Connection = new MySqlConnection("");
            Connection.Open();
        }

        ~ MapperTest()
        {
            Connection?.Close();
        }

        [Fact]
        public void GetUserTest()
        {
            var users = Connection.Query<User>("SELECT * FROM `Users` WHERE `UserName` LIKE @UserName", new { UserName = "1" });

            Assert.NotEmpty(users);
        }

        [Fact]
        public void GetOrderTest()
        {
            var orders = Connection.Query<Order, Category, Order>("SELECT * FROM `Orders` AS `O` INNER JOIN `Categories` AS `C` ON `C`.`Id` = `O`.`CategoryId` WHERE `O`.`Count` > 0 LIMIT @Skip,@Take",
                (o, c) =>
                {
                    o.Category = c;
                    return o;
                }, new { Skip = 0, Take = 10 });

            Assert.NotEmpty(orders);
            Assert.NotNull(orders.ElementAt(0).Category);
        }

        [Fact]
        public void GetUserWithManyOrdersTest()
        {
            var query = Connection.QueryMultiple("SELECT * FROM `Users` AS `U` WHERE `U`.`UserName` LIKE '%1%' LIMIT @Skip,@Take;SELECT * FROM(SELECT Id FROM `Users` AS `U` WHERE `U`.`UserName` LIKE '%1%' LIMIT @Skip, @Take) AS `U` INNER JOIN `Orders` AS `O` ON `U`.`Id` = `O`.`UserId`", new { Skip = 0, Take = 10 });

            var users = query.Read<User>().ToList();
            var orders = query.Read<Order>().ToList();
            foreach (var user in users)
            {
                user.Orders = orders.Where(x => x.UserId == user.Id).ToList();
            }

            Assert.NotEmpty(users);
            Assert.NotNull(users[0].Orders);
        }

        [Fact]
        public void GetUserWithManyOrdersIncludeCategoryTest()
        {
            var query = Connection.QueryMultiple("SELECT * FROM `Users` AS `U` WHERE `U`.`UserName` LIKE '%1%' LIMIT @Skip,@Take;SELECT * FROM(SELECT Id FROM `Users` AS `U` WHERE `U`.`UserName` LIKE '%1%' LIMIT @Skip, @Take) AS `U` INNER JOIN `Orders` AS `O` ON `U`.`Id` = `O`.`UserId` INNER JOIN `Categories` AS `C` ON `C`.`Id` = `O`.`CategoryId`", new { Skip = 0, Take = 10 });

            var users = query.Read<User>().ToList();
            var orders = query.Read<Order, Category, Order>((o, c) =>
            {
                o.Category = c;
                return o;
            }).ToList();
            foreach (var user in users)
            {
                user.Orders = orders.Where(x => x.UserId == user.Id).ToList();
            }

            Assert.NotEmpty(users);
            Assert.NotNull(users[0].Orders);
            Assert.NotNull(orders[0].Category);
        }
    }
}
