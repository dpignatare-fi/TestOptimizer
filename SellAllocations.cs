using System;
using System.Collections.Generic;
using System.Text;

namespace TestOptimizer
{
    //class SellAllocations
    //{
    //}

    public class CurrentAllocation
    {
        public long OrderId { get; set; }
        public long SecId { get; set; }

        public decimal TargetQty { get; set; }

        public decimal TargetBaseAmt { get; set; }

        //public SellAllocations(long orderid, long secid, decimal targetqty)
        //{
        //    OrderId = orderid;
        //    SecId = secid;
        //    TargetQty = targetqty;
        //}
    }

    public class TargetAllocation
    {
        public long OrderId { get; set; }
        public long SecId { get; set; }
        public string AcctCd { get; set; }
        public decimal TargetQty { get; set; }

        public decimal TargetBaseAmt { get; set; }


        //public SellAllocations(long orderid, long secid, decimal targetqty)
        //{
        //    OrderId = orderid;
        //    SecId = secid;
        //    TargetQty = targetqty;
        //}
    }
}
