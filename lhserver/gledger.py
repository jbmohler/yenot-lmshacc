import datetime
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

def get_api_transactions_account_summary_prompts():
    return api.PromptList(
            date1=api.cgen.date(label='Start Date', relevance=('date2', 'end-range', None)),
            date2=api.cgen.date(label='End Date'),
            account=api.cgen.pyhacc_account.id(),
            __order__=['date1', 'date2', 'account'])

@app.get('/api/transactions/account-summary', name='get_api_transactions_account_summary', \
        report_title='Transactions Account Summary', report_prompts=get_api_transactions_account_summary_prompts)
def get_api_transactions_account_summary():
    date1 = api.parse_date(request.query.get('date1'))
    date2 = api.parse_date(request.query.get('date2'))
    account = request.query.get('account')

    if date1 == None or date2 == None:
        raise api.UserError('parameter-validation', 'Enter both begin & end dates.')
    elif date1 > date2:
        raise api.UserError('parameter-validation', 'Start date must be before end date.')

    select = """
with memo_grouped as (
    select 
        transactions.payee, 
        transactions.memo, 
        sum(splits.sum) as debit
    from hacc.transactions
    join hacc.splits on splits.stid=transactions.tid
    join hacc.accounts on splits.account_id=accounts.id
    where /*WHERE*/
    group by payee, memo
    order by payee, memo
)
select payee, sum(debit) as debit, 
    array_agg(format('%%s (%%s)', memo, to_char(debit, 'FM9,999.90')) order by debit desc) as items
from memo_grouped
group by payee
"""

    wheres = [
            "transactions.trandate between %(d1)s and %(d2)s"]
    params = {
            'd1': date1, 
            'd2': date2}

    wheres.append("accounts.id=%(account)s")
    params['account'] = account

    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    results.key_labels += 'Date:  {} -- {}'.format(date1, date2)
    with app.dbconn() as conn:
        accname, isdebit = api.sql_1row(conn, "select acc_name, (select debit from hacc.accounttypes where id=accounts.type_id) as debit from hacc.accounts where id=%(s)s", {'s': account})
        results.key_labels += 'Account:  {}'.format(accname)

        cm = api.ColumnMap(
            debit=api.cgen.currency_usd(),
            credit=api.cgen.currency_usd(),
            items=api.cgen.stringlist())
        results.tables['payee', True] = api.sql_tab2(conn, select, params, cm)

    #results.keys['report-formats'] = ['gl_summarize_total']
    return results.json_out()

def get_api_transactions_recent_header_prompts():
    return api.PromptList(
            date1=api.cgen.date(label='Start Date', relevance=('date2', 'end-range', None)),
            date2=api.cgen.date(label='End Date'),
            __order__=['date1', 'date2'])

@app.get('/api/transactions/recent-header', name='api_transactions_recent_header', \
        report_title='Recent Transaction Sets', report_prompts=get_api_transactions_recent_header_prompts)
def get_api_transactions_recent_header():
    date1 = api.parse_date(request.query.get('date1'))
    date2 = api.parse_date(request.query.get('date2'))

    if date1 == None or date2 == None:
        raise api.UserError('parameter-validation', 'Enter both begin & end dates.')
    elif date1 > date2:
        raise api.UserError('parameter-validation', 'Start date must be before end date.')

    select = """
select 
    transactions.tid, transactions.trandate, 
    transactions.payee, transactions.memo
from hacc.transactions
where trandate between %(d1)s and %(d2)s
"""

    params = {
            'd1': date1, 
            'd2': date2}

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                tid=api.cgen.pyhacc_transaction.surrogate(),
                payee=api.cgen.pyhacc_transaction.link(url_key='tid'))
        results.tables['trans', True] = api.sql_tab2(conn, select, params, cm)

    return results.json_out()
