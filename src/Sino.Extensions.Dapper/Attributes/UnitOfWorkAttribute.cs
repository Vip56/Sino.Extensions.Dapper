using AspectCore.DynamicProxy;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace Sino.Extensions.Dapper.Attributes
{
    /// <summary>
    /// 工作单元
    /// </summary>
    public class UnitOfWorkAttribute : AbstractInterceptorAttribute
    {
        public async override Task Invoke(AspectContext context, AspectDelegate next)
        {
            try
            {
                using (TransactionScope scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                {
                    await next(context);
                    scope.Complete();
                }
            }
            catch (Exception ex)
            {
                //判断异常类型 如果是Sino异常则进行转发
                var sex = ex.InnerException as SinoException;
                if (sex!=null)
                    throw new SinoException(sex.Message, sex.Code);
                throw new Exception(ex.Message,ex);
            }
        }
    }
}
