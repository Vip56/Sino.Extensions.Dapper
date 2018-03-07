namespace Sino.Dapper
{
    public class DapperConfiguration : IDapperConfiguration
	{
        public string ReadConnectionString { get; set; }

        public string WriteConnectionString { get; set; }
    }
}