using Crd.Wsi;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace TestOptimizer
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
            txtAccountCode.Text = "COMB.UC.2";
            txtProfile.Text = "62951676";
        }

        private void button1_Click(object sender, EventArgs e)
        {

            //
            ClientSession session = Helper.createCRDSession();
            //run optimization profile
            OrderGenerationResult ogResult = TestOptimizer.Helper.runOptimizer(long.Parse(txtProfile.Text), txtAccountCode.Text, session);
            //fetch the generated trades
            while (Helper.runIdStatus(ogResult.runId, session) != "complete")
            {
                Thread.Sleep(3000);
            }
            //Thread.Sleep(20000);
            //Hashtable[] tradesCreated = TestOptimizer.Helper.fetchTradesByRunId(ogResult.runId, session);
            DataTable tradesCreated = TestOptimizer.Helper.fetchTradesByRunId(ogResult.runId, session);

            //MPO_DEMO
            var sleeveCodes = tradesCreated.AsEnumerable().Select(r => r.Field<string>("ACCT_CD")).Distinct().ToList();
            //Hashtable[] acctPositions = TestOptimizer.Helper.fetchPositions(txtAccountCode.Text, session);
            DataTable acctPositions = TestOptimizer.Helper.fetchPositions(sleeveCodes, session);
            TestOptimizer.Helper.distributeTradesToAccounts(tradesCreated, acctPositions, session, ogResult.runId);
         }
    }
}
