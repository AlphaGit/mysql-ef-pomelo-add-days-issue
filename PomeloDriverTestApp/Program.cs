using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace PomeloDriverTestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var services = new ServiceCollection();
            var serviceProvider = ConfigureServices(services);

            // Initial data setup.
            using (var context = serviceProvider.GetService<MyContext>())
            {
                context.Database.EnsureCreated();

                var category1 = new Category() { Id = Guid.NewGuid(), Name = "Test" };
                var category2 = new Category() { Id = Guid.NewGuid(), Name = "Test2" };

                var blogPost1 = new BlogPost() { Content = "Hello world", Category = category1, Id = Guid.NewGuid(), Title = "First post", PublicationDate = new DateTimeOffset(2018, 05, 10, 17, 25, 00, TimeSpan.Zero) };
                var blogPost2 = new BlogPost() { Content = "Second post", Category = category2, Id = Guid.NewGuid(), Title = "Second post", PublicationDate = new DateTimeOffset(2018, 05, 16, 17, 25, 00, TimeSpan.Zero) };
                var blogPost3 = new BlogPost() { Content = "Test", Category = category1, Id = Guid.NewGuid(), Title = "Test", PublicationDate = new DateTimeOffset(2018, 05, 16, 17, 25, 00, TimeSpan.Zero) };

                context.Add(blogPost1);
                context.Add(blogPost2);
                context.Add(blogPost3);
                context.SaveChanges();
            }

            //// Ensuring querying works fine
            //using (var context = new MyContext())
            //{
            //    var post = context.BlogPosts.First(p =>
            //        p.PublicationDate > DateTimeOffset.Parse("2017-01-01"));
            //    Console.WriteLine($"Test querying -- Post found: {post.Title}");
            //}

            //TrickyQuery1(serviceProvider).GetAwaiter().GetResult();
            //TrickyQuery2(serviceProvider).GetAwaiter().GetResult();
            //TrickyQuery3(serviceProvider).GetAwaiter().GetResult();
            MinimalExampleQuery(serviceProvider).GetAwaiter().GetResult();

            Console.ReadKey();
        }

        private static ServiceProvider ConfigureServices(ServiceCollection services)
        {
            services.AddLogging(configure => configure.AddConsole());
            services.AddDbContext<MyContext>(options =>
            {
                options.UseMySql(@"Server=localhost;database=ef;uid=root;pwd=123456;");
            }, ServiceLifetime.Transient);
            return services.BuildServiceProvider();
        }

        private static async Task TrickyQuery1(ServiceProvider serviceProvider)
        {
            using (var context = serviceProvider.GetService<MyContext>())
            {
                var daysDifferenceFromLatest = 5;
                var posts = await context.BlogPosts
                    .Where(p => p.Category.Name == p.Title)
                    .Where(p =>
                        p.PublicationDate < context.BlogPosts
                            .Where(p1 => p1.Category == p.Category)
                            .Max(p1 => p1.PublicationDate)
                            .AddDays(-daysDifferenceFromLatest))
                    .ToListAsync();
                Console.WriteLine($"Tricky querying -- Posts found: {posts.Count}");
            }

            /* Result:
                 warn: Microsoft.EntityFrameworkCore.Query[20502]
                       Possible unintended use of a potentially throwing aggregate method (Min, Max, Average) in a subquery.
                       Client evaluation will be used and operator will throw if no data exists.
                       Changing the subquery result type to a nullable type will allow full translation.
            */
        }

        private static async Task TrickyQuery2(ServiceProvider serviceProvider)
        {
            using (var context = serviceProvider.GetService<MyContext>())
            {
                var daysDifferenceFromLatest = 5;
                var posts = await context.BlogPosts
                    .Where(p => p.Category.Name == p.Title)
                    .Where(p =>
                        p.PublicationDate < context.BlogPosts
                            .Where(p1 => p1.Category == p.Category)
                            .OrderByDescending(p1 => p1.PublicationDate)
                            .Select(p1 => p1.PublicationDate)
                            .FirstOrDefault()
                            .AddDays(-daysDifferenceFromLatest))
                    .ToListAsync();
                Console.WriteLine($"Tricky querying -- Posts found: {posts.Count}");
            }

            /* Result:
                fail: Microsoft.EntityFrameworkCore.Database.Command[20102]
                   Failed executing DbCommand (18ms) [Parameters=[], CommandType='Text', CommandTimeout='30']
                   SELECT `p`.`Id`, `p`.`CategoryId`, `p`.`Content`, `p`.`PublicationDate`, `p`.`Title`
                   FROM `BlogPosts` AS `p`
                   LEFT JOIN `Category` AS `p.Category` ON `p`.`CategoryId` = `p.Category`.`Id`
                   WHERE ((`p.Category`.`Name` = `p`.`Title`) OR (`p.Category`.`Name` IS NULL AND `p`.`Title` IS NULL)) AND (`p`.`PublicationDate` < DATE_ADD((
                   SELECT `p1`.`PublicationDate`
                   FROM `BlogPosts` AS `p1`
                   WHERE (`p1`.`CategoryId` = `p`.`CategoryId`) OR (`p1`.`CategoryId` IS NULL AND `p`.`CategoryId` IS NULL)
                   ORDER BY `p1`.`PublicationDate` DESC
                   LIMIT 1
                   ), INTERVAL Convert(-__daysDifferenceFromLatest_0, Double) day))
                   MySql.Data.MySqlClient.MySqlException (0x80004005): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'Double) day))' at line 10 ---> MySql.Data.MySqlClient.MySqlException (0x80004005): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'Double) day))' at line 10            */
        }

        private static async Task TrickyQuery3(ServiceProvider serviceProvider)
        {
            using (var context = serviceProvider.GetService<MyContext>())
            {
                double daysDifferenceFromLatest = 5;
                var posts = await context.BlogPosts
                    .Where(p => p.Category.Name == p.Title)
                    .Where(p =>
                        p.PublicationDate < context.BlogPosts
                            .Where(p1 => p1.Category == p.Category)
                            .OrderByDescending(p1 => p1.PublicationDate)
                            .Select(p1 => p1.PublicationDate)
                            .FirstOrDefault()
                            .AddDays(-daysDifferenceFromLatest))
                    .ToListAsync();
                Console.WriteLine($"Tricky querying -- Posts found: {posts.Count}");
            }

            /* Result:
             fail: Microsoft.EntityFrameworkCore.Database.Command[20102]
               Failed executing DbCommand (19ms) [Parameters=[], CommandType='Text', CommandTimeout='30']
               SELECT `p`.`Id`, `p`.`CategoryId`, `p`.`Content`, `p`.`PublicationDate`, `p`.`Title`
               FROM `BlogPosts` AS `p`
               LEFT JOIN `Category` AS `p.Category` ON `p`.`CategoryId` = `p.Category`.`Id`
               WHERE ((`p.Category`.`Name` = `p`.`Title`) OR (`p.Category`.`Name` IS NULL AND `p`.`Title` IS NULL)) AND (`p`.`PublicationDate` < DATE_ADD((
               SELECT `p1`.`PublicationDate`
               FROM `BlogPosts` AS `p1`
               WHERE (`p1`.`CategoryId` = `p`.`CategoryId`) OR (`p1`.`CategoryId` IS NULL AND `p`.`CategoryId` IS NULL)
               ORDER BY `p1`.`PublicationDate` DESC
               LIMIT 1
               ), INTERVAL -__daysDifferenceFromLatest_0 day))
               MySql.Data.MySqlClient.MySqlException (0x80004005): Unknown column '__daysDifferenceFromLatest_0' in 'where clause' ---> MySql.Data.MySqlClient.MySqlException (0x80004005): Unknown column '__daysDifferenceFromLatest_0' in 'where clause'*/
        }

        private static async Task MinimalExampleQuery(ServiceProvider serviceProvider)
        {
            using (var context = serviceProvider.GetService<MyContext>())
            {
                double daysDiff = 5;
                var posts = await context.BlogPosts
                    .Where(p => p.PublicationDate.AddDays(daysDiff) < DateTimeOffset.Now)
                    .ToListAsync();
                Console.WriteLine($"Tricky querying -- Posts found: {posts.Count}");
            }

            /* Result:
             fail: Microsoft.EntityFrameworkCore.Database.Command[20102]
               Failed executing DbCommand (19ms) [Parameters=[], CommandType='Text', CommandTimeout='30']
               SELECT `p`.`Id`, `p`.`CategoryId`, `p`.`Content`, `p`.`PublicationDate`, `p`.`Title`
               FROM `BlogPosts` AS `p`
               LEFT JOIN `Category` AS `p.Category` ON `p`.`CategoryId` = `p.Category`.`Id`
               WHERE ((`p.Category`.`Name` = `p`.`Title`) OR (`p.Category`.`Name` IS NULL AND `p`.`Title` IS NULL)) AND (`p`.`PublicationDate` < DATE_ADD((
               SELECT `p1`.`PublicationDate`
               FROM `BlogPosts` AS `p1`
               WHERE (`p1`.`CategoryId` = `p`.`CategoryId`) OR (`p1`.`CategoryId` IS NULL AND `p`.`CategoryId` IS NULL)
               ORDER BY `p1`.`PublicationDate` DESC
               LIMIT 1
               ), INTERVAL -__daysDifferenceFromLatest_0 day))
               MySql.Data.MySqlClient.MySqlException (0x80004005): Unknown column '__daysDiff_0' in 'where clause' ---> MySql.Data.MySqlClient.MySqlException (0x80004005): Unknown column '__daysDifferenceFromLatest_0' in 'where clause'*/
        }

        class BlogPost
        {
            public Guid Id { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
            public Category Category { get; set; }
            public DateTimeOffset PublicationDate { get; set; }
        }

        class Tag
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
        }

        class Category
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
        }

        class MyContext : DbContext
        {
            public MyContext(DbContextOptions<MyContext> options): base(options)
            { }

            public DbSet<BlogPost> BlogPosts { get; set; }
        }
    }
}
