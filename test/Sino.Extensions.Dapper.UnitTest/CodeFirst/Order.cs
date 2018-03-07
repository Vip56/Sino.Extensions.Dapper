namespace Sino.Extensions.Dapper
{
	public class Order
	{
		public int Id { get; set; }

		public string Title { get; set; }

		public float Price { get; set; }

		public int Count { get; set; }

		public int UserId { get; set; }

		public User User { get; set; }

		public int CategoryId { get; set; }

		public Category Category { get; set; }
	}
}
