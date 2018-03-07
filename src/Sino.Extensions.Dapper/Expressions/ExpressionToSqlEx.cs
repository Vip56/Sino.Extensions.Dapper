using System;

namespace Sino.Extensions.Dapper.Expressions
{
    public static class ExpressionToSqlEx
    {
        public static bool Like(this object obj, string value)
        {
            return true;
        }

        /// <summary>
        /// like '% _ _ _'
        /// </summary>
        public static bool LikeLeft(this object obj, string value)
        {
            return true;
        }

        /// <summary>
        /// like '_ _ _ %'
        /// </summary>
        public static bool LikeRight(this object obj, string value)
        {
            return true;
        }

		public static bool In<T>(this object obj, params T[] ary) where T : IComparable
		{
			return true;
		}
    }
}
