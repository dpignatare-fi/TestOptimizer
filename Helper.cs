using Crd.Wsi;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace TestOptimizer
{
    static class Helper
    {
        private static  OrderGenOptimizerBean[] getOptimerBeanByGenId(Hashtable[] resulsetResult)
        {
            List<OrderGenOptimizerBean> orderGenOptimizerBeans = new List<OrderGenOptimizerBean>();

            foreach(Hashtable optimimizerHash in resulsetResult)
            {
                OrderGenOptimizerBean optimizerBean = new OrderGenOptimizerBean(true,true);
            }

            return orderGenOptimizerBeans.ToArray();
        }

        public static OrderGenerationResult runOptimizer(long profileId, string acctCd, ClientSession session)
        {
            OrderGenerationService orderGenerationService = new OrderGenerationServiceImpl(session);
            WMOrderGenerationRequest orderGenerationRequest = new WMOrderGenerationRequest(true, true);
            OrderGenWebService ogWS = new OrderGenWebServiceImpl(session);
            OrderGenOptimizerWebService ogoptWS = new OrderGenOptimizerWebServiceImpl(session);
            OrderGenBean ogBean = ogWS.fetchById(profileId);
            ogBean.acctCd = acctCd;
            Parameter param = new Parameter { name = "ordergenid", numberValue = profileId };
            Hashtable[] test = session.GetIMSUtilitiesService().fetchResultSet("optGenBeans2", null, null, 0L, new Parameter[] { param }).ToArray();
            OrderGenOptimizerBean[] optBean = ogoptWS.fetch("orderGenOptBeans", null, 0L, new Parameter[] { param });


            OrderGenOptimizerConstraintWebService ogoptConstraint = new OrderGenOptimizerConstraintWebServiceImpl(session);
            OrderGenOptimizerConstraintBean[] optConstraints = ogoptConstraint.fetch("optConstraintsBeans", null, 0L, new Parameter[] { param });
            OrderGenOptimizerObjectiveWebService ogoptCObjective = new OrderGenOptimizerObjectiveWebServiceImpl(session);
            OrderGenOptimizerObjectiveBean[] optObjectives = ogoptCObjective.fetch("optObjectivesBeans", "", 0L, new Parameter[] { param });
            OrderGenOptimizerSecurityUniverseWebService ogoptSecUniverse = new OrderGenOptimizerSecurityUniverseWebServiceImpl(session);
            OrderGenOptimizerSecurityUniverseBean[] optSecUniverse = ogoptSecUniverse.fetch("optSecurityUnivBeans", "", 0L, new Parameter[] { param });
            optBean[0].optimizerConstraintBeans = optConstraints;
            optBean[0].optimizerObjectiveBeans = optObjectives;
            optBean[0].optimizerSecurityUniverseBeans = optSecUniverse;


            ogBean.genOptimizerBean = optBean[0];
            orderGenerationRequest.orderGen = ogBean;
            WMOrderGenHistoryAndSaveParams ogHistParams = new WMOrderGenHistoryAndSaveParams(true, true);

            ogHistParams.orderGenSource = WMOrderGenSource.OPTIMIZER;
            WMRequestDetailSpec ogReqDetailSpec = new WMRequestDetailSpec(true, true);
            ogReqDetailSpec.useDetailInOrderGen = false;
            ogReqDetailSpec.accountCode = acctCd;

            OrderGenerationResult ogResult = orderGenerationService.generateOrderAllocations("test", orderGenerationRequest, ogHistParams, new WMRequestDetailSpec[] { ogReqDetailSpec });
            return ogResult;
        }

        public static DataTable fetchTradesByRunId(long runId, ClientSession session)
        {
            Parameter parameter = new Parameter("runId", runId);
            List<Parameter> paramList = new List<Parameter>();
            paramList.Add(parameter);
            DataTable dtOut = new DataTable();
            //SELECT O.ORDER_GEN_RUN_ID, ACCT_CD, OA.TARGET_QTY, OA.TARGET_AMT, TARGET_BASE_AMT, S.SEC_NAME, S.EXT_SEC_ID, S.TICKER, S.SEC_ID
            dtOut.Columns.Add("ACCT_CD", System.Type.GetType("System.String"));
            dtOut.Columns.Add("TARGET_BASE_AMT", System.Type.GetType("System.Decimal"));
            dtOut.Columns.Add("EXT_SEC_ID", System.Type.GetType("System.String"));
            dtOut.Columns.Add("TRANS_TYPE", System.Type.GetType("System.String"));
            dtOut.Columns.Add("SEC_ID", System.Type.GetType("System.Int64"));
            dtOut.Columns.Add("TRADE_ID", System.Type.GetType("System.Int64"));
            dtOut.Columns.Add("ORDER_ID", System.Type.GetType("System.Int64"));
            dtOut.Columns.Add("TARGET_QTY", System.Type.GetType("System.Decimal"));
            dtOut.Columns.Add("ORDER_QTY", System.Type.GetType("System.Decimal"));
            Hashtable map = new Hashtable();
            map["ACCT_CD"] = "ACCT_CD";
            map["TARGET_BASE_AMT"] = "TARGET_BASE_AMT";
            map["EXT_SEC_ID"] = "EXT_SEC_ID";
            map["SEC_ID"] = "SEC_ID";
            map["TARGET_QTY"] = "TARGET_QTY";
            map["TRADE_ID"] = "TRADE_ID";
            map["ORDER_ID"] = "ORDER_ID";
            map["ORDER_QTY"] = "ORDER_QTY";
            map["TRANS_TYPE"] = "TRANS_TYPE";

            dtOut = session.GetIMSUtilitiesService().fetchResultSet("com.fi.TradesByRundId", null, null, 0L, paramList.ToArray()).ToDataTable(dtOut, map);

            return dtOut;
        }

        public static DataTable fetchPositions(List<string> acctCds, ClientSession session)
        {
            List<Parameter> paramList = new List<Parameter>();
            foreach (string acctCd in acctCds)
            {
                Parameter parameter = new Parameter("acctCd", acctCd);
                paramList.Add(parameter);

            }
            DataTable dtOut = new DataTable();
            dtOut.Columns.Add("ACCT_CD", System.Type.GetType("System.String"));
            dtOut.Columns.Add("QTY_SOD", System.Type.GetType("System.Decimal"));
            dtOut.Columns.Add("QTY_INTRADAY", System.Type.GetType("System.Decimal"));
            dtOut.Columns.Add("EXT_SEC_ID", System.Type.GetType("System.String"));
            dtOut.Columns.Add("TICKER", System.Type.GetType("System.String"));
            dtOut.Columns.Add("SEC_NAME", System.Type.GetType("System.String"));
            dtOut.Columns.Add("SEC_TYP_CD", System.Type.GetType("System.String"));
            dtOut.Columns.Add("SEC_ID", System.Type.GetType("System.Int64"));
            dtOut.Columns.Add("MKT_VAL_SOD", System.Type.GetType("System.Decimal"));
            dtOut.Columns.Add("MKT_VAL_INTRADAY", System.Type.GetType("System.Decimal"));
            dtOut.Columns.Add("TOTAL_MKT_VAL", System.Type.GetType("System.Decimal"));

            Hashtable map = new Hashtable();
            map["ACCT_CD"] = "ACCT_CD";
            map["QTY_SOD"] = "QTY_SOD";
            map["EXT_SEC_ID"] = "EXT_SEC_ID";
            map["SEC_ID"] = "SEC_ID";
            map["QTY_INTRADAY"] = "QTY_INTRADAY";
            map["TICKER"] = "TICKER";
            map["SEC_NAME"] = "SEC_NAME";
            map["SEC_TYP_CD"] = "SEC_TYP_CD";
            map["MKT_VAL_SOD"] = "MKT_VAL_SOD";
            map["MKT_VAL_SOD"] = "MKT_VAL_SOD";
            map["TOTAL_MKT_VAL"] = "TOTAL_MKT_VAL";

            dtOut = session.GetIMSUtilitiesService().fetchResultSet("com.fi.PositionsForAccountList", null, null, 0L, paramList.ToArray()).ToDataTable(dtOut,map);
            return dtOut;

        }

        public static string runIdStatus(long runId, ClientSession session)
        {
            Parameter parameter = new Parameter("runId", runId);
            List<Parameter> paramList = new List<Parameter>();
            paramList.Add(parameter);
            Hashtable[] dtOut = session.GetIMSUtilitiesService().fetchResultSet("com.fi.RunIdStatus", null, null, 0L, paramList.ToArray()).ToArray();
            string status = (dtOut.Length > 0) ? "complete" : "running";
            return status;
        }

        public static void distributeTradesToAccounts(DataTable trades, DataTable positions, ClientSession session, long runId)
        {
            Dictionary<string, decimal> dictCashBySleeve = new Dictionary<string, decimal>();

            var sleeveCodes = trades.AsEnumerable().Select(r => r.Field<string>("ACCT_CD")).Distinct();
            
            // 
            foreach(string sleeveCode in sleeveCodes)
            {
                dictCashBySleeve.Add(sleeveCode, 0);
            }

            // get all tradeIds by orderid
            var tradeIds = trades.AsEnumerable().Select(g => new
            {
                OrderId = g.Field<long>("ORDER_ID"),
                TradeId = g.Field<long>("TRADE_ID")
            })
            ;

            // get sell allocations
            var sellTrades = trades.AsEnumerable()
                          .Where(row => row["TRANS_TYPE"].ToString() == "SELLL");

            // get all sell securities
            var sellSecIds = sellTrades.AsEnumerable().Select(s => s.Field<long>("SEC_ID") );

            // get all positions for sell securities
            var positionsSellSecurities = positions.AsEnumerable().Where(row => sellSecIds.Contains(long.Parse(row["SEC_ID"].ToString())));
            Debug.WriteLine("Positions for Sell Securities");
            Debug.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(positionsSellSecurities, Formatting.Indented));
            // get sell trades grouped by secid
            var sellTradesDescending = trades.AsEnumerable()
                .Where(row => row["TRANS_TYPE"].ToString() == "SELLL")
                .GroupBy(r => new
                {
                    SEC_ID = r.Field<long>("SEC_ID")
                })
                .Select(g => new CurrentAllocation
                {
                    OrderId = g.First().Field<long>("ORDER_ID"),
                    SecId = g.First().Field<long>("SEC_ID"),
                    TargetQty = g.Sum(x => x.Field<decimal>("TARGET_QTY")),
                    TargetBaseAmt = g.Sum(x => x.Field<decimal>("TARGET_BASE_AMT"))
                }); ;
            Debug.WriteLine("Sell Trades - block");
            Debug.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(sellTradesDescending, Formatting.Indented));
            // get new sell allocations
            var newSellAllocations = getSellAllocations(positionsSellSecurities.ToList(), sellTradesDescending.ToList()).OrderBy(x=>x.OrderId);
            Debug.WriteLine("New Sell Allocations");
            Debug.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(newSellAllocations));
            // get buy trades by secid
            var buyTradesDescending = trades.AsEnumerable()
                .Where(row => row["TRANS_TYPE"].ToString() == "BUYL")
                .GroupBy(r => new
                    {
                        SEC_ID = r.Field<long>("SEC_ID")
                    })
                .Select(g => new CurrentAllocation
                {
                    OrderId = g.First().Field<long>("ORDER_ID"),
                    SecId = g.First().Field<long>("SEC_ID"),
                    TargetQty = g.Sum(x => x.Field<decimal>("TARGET_QTY")),
                    TargetBaseAmt = g.Sum(x => x.Field<decimal>("TARGET_BASE_AMT"))
                })
                .OrderByDescending(x => x.TargetBaseAmt);
            Debug.WriteLine("Buy Trades - block");
            Debug.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(buyTradesDescending, Formatting.Indented));
            // get position sleeve cash
            var cashBySleeves = positions.AsEnumerable()
                          .Where(row => row["SEC_TYP_CD"].ToString() == "CURR")
                          .GroupBy(r => new
                          {
                              ACCT_CD = r.Field<string>("ACCT_CD")
                          })
                          .Select(g => new
                          {
                              AcctCd = g.First().Field<string>("ACCT_CD"),
                              SumTotalCash = g.Sum(x => x.Field<decimal>("TOTAL_MKT_VAL"))
                          })
                          .OrderByDescending(x => x.SumTotalCash);
            
            // add position sleeve cash
            foreach(var cashBySleeve in cashBySleeves)
            {
                dictCashBySleeve[cashBySleeve.AcctCd] += cashBySleeve.SumTotalCash;
            }

            
            // get trade sleeve cash
            var sellCashByAcct = newSellAllocations.GroupBy(r => r.AcctCd).Select(g => new { AcctCd = g.First().AcctCd, SumCash = g.Sum(x => x.TargetBaseAmt) });
            //foreach(var allocation in newSellAllocations)
            //{
            //    Debug.WriteLine(allocation.AcctCd + "," + allocation.SecId.ToString() + "," + allocation.TargetQty.ToString() + "," + allocation.TargetBaseAmt.ToString());
            //}

            // add sell trade sleeve cash
            foreach(var sellCash in sellCashByAcct)
            {

                dictCashBySleeve[sellCash.AcctCd] += sellCash.SumCash;
            }
            Debug.WriteLine("Cash By Sleeve");
            Debug.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(dictCashBySleeve, Formatting.Indented));

            // get reallocated buys (cash, block trade qty)
            var newBuyAllocations = getBuyAllocations(dictCashBySleeve, buyTradesDescending.ToList()).OrderByDescending(x=>x.TargetBaseAmt);
            Debug.WriteLine("New Buy Allocations");
            Debug.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(newBuyAllocations, Formatting.Indented));
            if ((newBuyAllocations.Count() + newSellAllocations.Count()) == 0 )
            {
                return;
            }

            // reallocate orders
            StringBuilder ordersUpdate = new StringBuilder();
            ordersUpdate.AppendLine("<envelope><trading op=\"updateOrder\" id=\"intechTrading\">");
            if (newBuyAllocations.Count() > 0)
            {
                long newOrderId = 0;
                long passNumber = 0;
                foreach (var buyTrade in newBuyAllocations)
                {
                    var existingBuyAllocations = tradeIds.Where(r => r.OrderId == buyTrade.OrderId).Select(r => r.TradeId);
                    bool isNewOrder = buyTrade.OrderId != newOrderId;
                    if (passNumber == 0)
                    {

                        ordersUpdate.AppendLine("<order op=\"update\" crdId=\"" + buyTrade.OrderId.ToString() + "\">");
                        foreach (var existingBuyAllocation in existingBuyAllocations)
                        {
                            ordersUpdate.AppendLine("<allocation crdId=\"" + existingBuyAllocation.ToString() + "\" op=\"delete\" />");
                        }
                    }
                    else
                    {
                        if (newOrderId != buyTrade.OrderId)
                        {
                            ordersUpdate.AppendLine("</order>");
                            ordersUpdate.AppendLine("<order op=\"update\" crdId=\"" + buyTrade.OrderId.ToString() + "\">");
                            foreach (var existingBuyAllocation in existingBuyAllocations)
                            {
                                ordersUpdate.AppendLine("<allocation crdId=\"" + existingBuyAllocation.ToString() + "\" op=\"delete\" />");
                            }
                        }

                    };

                    ordersUpdate.AppendLine("<allocation id=\"" + Guid.NewGuid().ToString() + "\" op=\"create\">");
                    ordersUpdate.AppendLine("<acctCd>" + buyTrade.AcctCd + "</acctCd>");
                    ordersUpdate.AppendLine("<orderGenRunId>" + runId.ToString() + "</orderGenRunId>");
                    ordersUpdate.AppendLine("<targetQty>" + buyTrade.TargetQty + "</targetQty>");
                    ordersUpdate.AppendLine("</allocation>");
                    newOrderId = buyTrade.OrderId;
                    passNumber++;

                }
                ordersUpdate.AppendLine("</order>");
            }


            if(newSellAllocations.Count() > 0)
            { 
                long newOrderId = 0;
                long passNumber = 0;
                foreach (var sellTrade in newSellAllocations)
                {
                    var existingSellAllocations = tradeIds.Where(r => r.OrderId == sellTrade.OrderId).Select(r => r.TradeId);
                    bool isNewOrder = sellTrade.OrderId != newOrderId;
                    if (passNumber == 0)
                    {

                        ordersUpdate.AppendLine("<order op=\"update\" crdId=\"" + sellTrade.OrderId.ToString() + "\">");
                        foreach (var existingSellAllocation in existingSellAllocations)
                        {
                            ordersUpdate.AppendLine("<allocation crdId=\"" + existingSellAllocation.ToString() + "\" op=\"delete\" />");
                        }
                    }
                    else
                    {
                        if (newOrderId != sellTrade.OrderId)
                        {
                            ordersUpdate.AppendLine("</order>");
                            ordersUpdate.AppendLine("<order op=\"update\" crdId=\"" + sellTrade.OrderId.ToString() + "\">");
                            foreach (var existingSellAllocation in existingSellAllocations)
                            {
                                ordersUpdate.AppendLine("<allocation crdId=\"" + existingSellAllocation.ToString() + "\" op=\"delete\" />");
                            }
                        }

                    };

                    ordersUpdate.AppendLine("<allocation id=\"" + Guid.NewGuid().ToString() + "\" op=\"create\">");
                    ordersUpdate.AppendLine("<acctCd>" + sellTrade.AcctCd + "</acctCd>");
                    ordersUpdate.AppendLine("<targetQty>" + sellTrade.TargetQty + "</targetQty>");
                    ordersUpdate.AppendLine("<orderGenRunId>" + runId.ToString() + "</orderGenRunId>");
                    ordersUpdate.AppendLine("</allocation>");
                    newOrderId = sellTrade.OrderId;
                    passNumber++;
                }
                ordersUpdate.AppendLine("</order>");
            }

            ordersUpdate.AppendLine("</trading></envelope>");
            // do stuff in here
            string result = session.GetMessagingService().send(ordersUpdate.ToString());
            
        }

        public static List<TargetAllocation> getSellAllocations(List<DataRow> sellPositions, List<CurrentAllocation> sellAllocs)
        {
            List<TargetAllocation> targetAllocs = new List<TargetAllocation>();
            foreach (var sellAllocation in sellAllocs)
            {
                var positionsBySecId = sellPositions
                    .Where(row => long.Parse(row["SEC_ID"].ToString()) == sellAllocation.SecId)
                    .OrderBy(row => decimal.Parse(row["QTY_SOD"].ToString()));
                decimal targetRemainder = sellAllocation.TargetQty;
                
                foreach (var positionBySecid in positionsBySecId)
                {
                    if (targetRemainder > 0)
                    {
                        TargetAllocation targetAlloc = new TargetAllocation();
                        decimal posQty = decimal.Parse(positionBySecid["QTY_SOD"].ToString());
                        decimal qtyToTarget = posQty <= targetRemainder ? posQty : targetRemainder;
                        targetAlloc.AcctCd = positionBySecid["ACCT_CD"].ToString();
                        targetAlloc.TargetQty = qtyToTarget;
                        targetAlloc.SecId = sellAllocation.SecId;
                        targetAlloc.OrderId = sellAllocation.OrderId;
                        targetAlloc.TargetBaseAmt = (qtyToTarget / sellAllocation.TargetQty) * sellAllocation.TargetBaseAmt;
                        targetRemainder -= qtyToTarget;
                        targetAllocs.Add(targetAlloc);
                    }
                    
                }
                
            }
            return targetAllocs;
            
        }

        public static List<TargetAllocation> getBuyAllocations(Dictionary<string,decimal> dictCashBySleeve, List<CurrentAllocation> buyAllocs)
        {

            List<TargetAllocation> targetAllocs = new List<TargetAllocation>();
            foreach (var buyAllocation in buyAllocs)
            {
                var dictCashByDescending = dictCashBySleeve.OrderByDescending(x => x.Value);
                string acctCdtoAllocation = dictCashByDescending.First().Key;
                decimal targetRemainder = buyAllocation.TargetQty;
                decimal targetBaseRemainder = buyAllocation.TargetBaseAmt;
                
                    if (targetRemainder > 0)
                    {
                        TargetAllocation targetAlloc = new TargetAllocation();
                        decimal qtyToTarget = dictCashBySleeve[acctCdtoAllocation] <= targetBaseRemainder ? Math.Floor((dictCashBySleeve[acctCdtoAllocation] / targetBaseRemainder) * targetRemainder) : targetRemainder;
                        targetAlloc.AcctCd = acctCdtoAllocation;
                        targetAlloc.TargetQty = qtyToTarget;
                        targetAlloc.SecId = buyAllocation.SecId;
                        targetAlloc.OrderId = buyAllocation.OrderId;
                        targetAlloc.TargetBaseAmt = (qtyToTarget / buyAllocation.TargetQty) * buyAllocation.TargetBaseAmt;
                        targetRemainder -= qtyToTarget;
                        dictCashBySleeve[acctCdtoAllocation] -= (qtyToTarget / buyAllocation.TargetQty) * buyAllocation.TargetBaseAmt;
                        targetAllocs.Add(targetAlloc);
                }
                
            }
            return targetAllocs;

        }
        public static void preProcessOptimization(string combinedAcctCd, ClientSession session)
        {
            // this is where will set the state of the accounts before running optimization
        }
        public static ClientSession createCRDSession()
        {
            ClientSession session = new ClientSession(TestOptimizer.Properties.Settings.Default.protocol, TestOptimizer.Properties.Settings.Default.host, TestOptimizer.Properties.Settings.Default.port);
            session.GetLogonService().logon(TestOptimizer.Properties.Settings.Default.uname, TestOptimizer.Properties.Settings.Default.pwd);
            byte[] privateBytes = System.Text.ASCIIEncoding.ASCII.GetBytes("EnablePrivateAccess");
            PrivateAccess.Enable(privateBytes);
            return session;
        }

    }
}

