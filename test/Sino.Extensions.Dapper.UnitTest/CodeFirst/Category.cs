using System.Collections.Generic;

namespace Sino.Extensions.Dapper
{
	public class Category
	{
		public int Id { get; set; }

		public string Name { get; set; }

		public virtual IList<Order> Orders { get; set; }
	}
}
