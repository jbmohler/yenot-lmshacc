import uuid
import datetime
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/transactions/years', name='get_api_transactions_years', \
        report_title='Transaction Years')
def get_api_transactions_years():
    select = """
select date_part('year', trandate)::int as year, count(*)
from hacc.transactions
group by date_part('year', trandate)::int
order by date_part('year', trandate)::int
"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        results.tables['years', True] = api.sql_tab2(conn, select, None, None)
    return results.json_out()

def get_api_transactions_list_prompts():
    return api.PromptList(
            date1=api.cgen.date(label='Start Date', relevance=('date2', 'end-range', None)),
            date2=api.cgen.date(label='End Date'),
            account=api.cgen.pyhacc_account.id(optional=True),
            acctype=api.cgen.pyhacc_accounttype.id(label='Account Type', optional=True),
            payee_frag=api.cgen.basic(label='Payee Search', optional=True),
            memo_frag=api.cgen.basic(label='Memo Search', optional=True),
            __order__=['date1', 'date2', 'account', 'acctype', 'payee_frag', 'memo_frag'])

@app.get('/api/transactions/list', name='get_api_transactions_list', \
        report_title='Transactions List', report_prompts=get_api_transactions_list_prompts)
def get_api_transactions_list():
    account = request.query.get('account')
    acctype = request.query.get('acctype')
    memo_frag = request.query.get('memo_frag', None)
    payee_frag = request.query.get('payee_frag', None)
    date1 = api.parse_date(request.query.get('date1'))
    date2 = api.parse_date(request.query.get('date2'))

    if date1 == None or date2 == None:
        raise api.UserError('parameter-validation', 'Enter both begin & end dates.')
    elif date1 > date2:
        raise api.UserError('parameter-validation', 'Start date must be before end date.')

    select = """
select 
    transactions.tid, 
    transactions.trandate as date, 
    transactions.tranref as reference, 
    accounts.id, accounts.acc_name,
    transactions.payee, 
    transactions.memo, 
    case when splits.sum>=0 then splits.sum end as debit,
    case when splits.sum<0 then -splits.sum end as credit
from hacc.transactions
join hacc.splits on splits.stid=transactions.tid
join hacc.accounts on splits.account_id=accounts.id
where /*WHERE*/
order by transactions.trandate, transactions.tranref, 
    transactions.payee, transactions.memo, accounts.acc_name
"""

    wheres = [
            "transactions.trandate between %(d1)s and %(d2)s"]
    params = {
            'd1': date1, 
            'd2': date2}

    if account != None:
        wheres.append("accounts.id=%(account)s")
        params['account'] = account

    if acctype != None:
        wheres.append("accounts.type_id=%(acctype)s")
        params['acctype'] = acctype

    if memo_frag not in ['', None]:
        wheres.append("transactions.memo ilike %(mf)s")
        params['mf'] = api.sanitize_fragment(memo_frag)

    if payee_frag not in ['', None]:
        wheres.append("transactions.payee ilike %(pf)s")
        params['pf'] = api.sanitize_fragment(payee_frag)

    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    results.key_labels += 'Date:  {} -- {}'.format(date1, date2)
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            tid=api.cgen.pyhacc_transaction.surrogate(),
            id=api.cgen.pyhacc_account.surrogate(),
            acc_name=api.cgen.pyhacc_account.name(url_key='id', label='Account', hidden=(account!=None)),
            debit=api.cgen.currency_usd(),
            credit=api.cgen.currency_usd())
        results.tables['trans', True] = api.sql_tab2(conn, select, params, cm)
        if account != None:
            accname = api.sql_1row(conn, "select acc_name from hacc.accounts where id=%(s)s", {'s': account})
            results.key_labels += 'Account:  {}'.format(accname)
    results.keys['report-formats'] = ['gl_summarize_total']
    return results.json_out()

def _get_api_transaction(tid=None, newrow=False):
    select = """
select *
from hacc.transactions
where /*WHERE*/"""

    selectdet = """
select splits.sid, splits.stid, 
    splits.account_id, accounts.acc_name,
    splits.sum,
    journals.jrn_name
from hacc.transactions
join hacc.splits on splits.stid=transactions.tid
join hacc.accounts on accounts.id=splits.account_id
left outer join hacc.journals on journals.id=accounts.journal_id
where /*WHERE*/"""

    params = {}
    wheres = []
    if tid != None:
        wheres.append("transactions.tid=%(t)s")
        params['t'] = tid
    if newrow:
        wheres.append("False")

    select = select.replace("/*WHERE*/", wheres[0])
    selectdet = selectdet.replace("/*WHERE*/", wheres[0])

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select, params)
        if newrow:
            def tran_default(index, row):
                row.tid = str(uuid.uuid1())
                row.trandate = datetime.date.today()
            rows = api.tab2_rows_default(columns, [None], tran_default)
        results.tables['trans'] = columns, rows

        cm = api.ColumnMap(
                sid=api.cgen.pyhacc_transaction.surrogate(),
                stid=api.cgen.pyhacc_transaction.surrogate(),
                account_id=api.cgen.pyhacc_account.surrogate(),
                sum=api.cgen.currency_usd())
        results.tables['splits'] = api.sql_tab2(conn, selectdet, params, cm)
    return results

@app.get('/api/transaction/new', name='get_api_transaction_new')
def get_api_transaction_new():
    results = _get_api_transaction(newrow=True)
    results.keys['new_row'] = True
    return results.json_out()

@app.get('/api/transaction/<t_id>', name='get_api_transaction')
def get_api_transaction(t_id):
    results = _get_api_transaction(tid=t_id)
    return results.json_out()

@app.put('/api/transaction/<t_id>', name='put_api_transaction')
def put_api_transaction(t_id):
    trans = api.table_from_tab2('trans', amendments=['tid'], allow_extra=True)
    splits = api.table_from_tab2('splits', amendments=['stid', 'sid'], required=['account_id', 'sum'])

    for row in trans.rows:
        row.tid = t_id
    for row in splits.rows:
        row.stid = t_id
        row.sid = None

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('hacc.transactions', trans)
            w.upsert_rows('hacc.splits', splits)
        conn.commit()
    return api.Results().json_out()
