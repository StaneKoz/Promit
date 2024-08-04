using Microsoft.Data.SqlClient;
using System.Configuration;

namespace Promit
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            string filePath = "";
            if (args.Length == 0)
            {
                Console.WriteLine("Не былы переданы аргументы командной строки, поэтому введите путь к файлу:");
                filePath = Console.ReadLine() ?? "";
            }
            else
            {
                filePath = args[0];
            }

            var file = await File.ReadAllTextAsync(filePath);
            var filteredWords = ValidateWords(file);
            await InitializeDatabase();
            await UpdateTable(filteredWords);
        }

        private static async Task UpdateRecord(KeyValuePair<string, int> record, SqlConnection connection)
        {
            string updateQuery = "UPDATE words SET count = count + @count WHERE word = @word";

            using (var updateCmd = new SqlCommand(updateQuery, connection))
            {
                updateCmd.Parameters.AddWithValue("@count", record.Value);
                updateCmd.Parameters.AddWithValue("@word", record.Key);

                await updateCmd.ExecuteNonQueryAsync();
            }
        }

        private static async Task CreateRecord(KeyValuePair<string, int> record, SqlConnection connection)
        {
            string insertQuery = "INSERT INTO Words (word, count) VALUES (@word, @count)";

            using (var insertCmd = new SqlCommand(insertQuery, connection))
            {
                insertCmd.Parameters.AddWithValue("@word", record.Key);
                insertCmd.Parameters.AddWithValue("@count", record.Value);

                await insertCmd.ExecuteNonQueryAsync();
            }
        }

        private static async Task UpdateTable(Dictionary<string, int> filteredWords)
        {

            var connectionString = ConfigurationManager.ConnectionStrings["PromitConnection"].ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                foreach (var word in filteredWords)
                {
                    var checkQuery = "SELECT COUNT(*) FROM words WHERE word = @word";

                    var checkCmd = new SqlCommand(checkQuery, connection);
                    checkCmd.Parameters.AddWithValue("@word", word.Key);

                    int count = (int)checkCmd.ExecuteScalar();
                    if (count > 0)
                    {
                        await UpdateRecord(word, connection);
                    }

                    else
                    {
                        await CreateRecord(word, connection);
                    }
                }
            }
        }

        private static async Task CreateDbIfNotExists(SqlConnection connection)
        {
            var sqlCommandText = @"IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = 'PROMIT')
                    BEGIN
                        CREATE DATABASE PROMIT
                    END;
                    ";

            var commandCreateDb = new SqlCommand(sqlCommandText, connection);

            await commandCreateDb.ExecuteNonQueryAsync();
        }

        private static async Task CreateTableIfNotExists()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["PromitConnection"].ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                var sqlCommandText = @"IF OBJECT_ID(N'dbo.words', N'U') IS NULL
                CREATE TABLE dbo.words (
                 id INT PRIMARY KEY IDENTITY,
                 word nvarchar(20) NOT NULL,
                 count INT NOT NULL);";

                var commandCreateTable = new SqlCommand(sqlCommandText, connection);

                await commandCreateTable.ExecuteNonQueryAsync();
            }
        }

        private static async Task InitializeDatabase()
        {
            var connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await CreateDbIfNotExists(connection);
                await CreateTableIfNotExists();
            }
        }

        private static Dictionary<string, int> ValidateWords(string text)
        {
            var words = text.Split(' ', '\n', '\r').Where(word => word != "").ToList();
            var result = new Dictionary<string, int>();

            foreach (var word in words)
            {
                if (word.Length < 3 || word.Length > 20)
                {
                    continue;
                }

                if (result.ContainsKey(word))
                {
                    result[word]++;
                }
                else
                {
                    result.Add(word, 1);
                }
            }

            return result.Where(keyPair => keyPair.Value >= 4).ToDictionary();
        }
    }
}