using System;
using System.Collections.Generic;

namespace Sino.Extensions.Dapper
{
    public class User
	{
		public int Id { get; set; }

		public DateTime CreationTime { get; set; }

		public string UserName { get; set; }

		public Sex Sex { get; set; }

		public virtual IList<Order> Orders { get; set; }
	}

	public enum Sex
	{
		Man,
		Woman
	}
}
