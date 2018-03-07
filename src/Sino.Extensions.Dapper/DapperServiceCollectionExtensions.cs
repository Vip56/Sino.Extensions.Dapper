using Sino.Dapper;

namespace Microsoft.Extensions.DependencyInjection
{
	public static class DapperServiceCollectionExtensions
	{
		public static IServiceCollection AddDapper(this IServiceCollection services, string writeConnectionString, string readConnectionString)
		{
			services.AddSingleton<IDapperConfiguration>(new DapperConfiguration
			{
				WriteConnectionString = writeConnectionString,
                ReadConnectionString = readConnectionString
            });
			return services;
		}
	}
}
