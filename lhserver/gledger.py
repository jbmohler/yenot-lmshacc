import datetime
import re
import boa
import rtlib
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

def get_api_gledger_unbalanced_trans_prompts():
    return api.PromptList(\
            __order__=[])

@app.get('/api/gledger/unbalanced-trans', name='api_gledger_unbalanced_trans', \
        report_title='Unbalanced Transactions', report_prompts=get_api_gledger_unbalanced_trans_prompts)
def get_api_gledger_unbalanced_trans():
    select = """
select 
    transactions.tid, transactions.payee, transactions.memo, transactions.trandate, journals.jrn_name, 
    sum(splits.sum) as unbalance
from hacc.accounts
join hacc.splits on splits.account_id=accounts.id
join hacc.transactions on transactions.tid=splits.stid
join hacc.journals on journals.id=accounts.journal_id
group by transactions.tid, transactions.payee, transactions.memo, transactions.trandate, journals.jrn_name
having sum(splits.sum)<>0
"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                tid=api.cgen.pyhacc_transaction.surrogate(),
                jrn_name=api.cgen.pyhacc_journal.name(label='Journal'))
        params = {}
        results.tables['balances', True] = api.sql_tab2(conn, select, params, cm)

    return results.json_out()
