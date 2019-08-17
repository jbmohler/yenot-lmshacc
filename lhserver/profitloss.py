import datetime
import re
import boa
import rtlib
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()

def dc_values(debit, debit_amt):
    d = debit_amt if debit else None
    c = -debit_amt if not debit else None
    return d, c

def dcb_values(debit, debit_amt):
    d = debit_amt if debit else None
    c = -debit_amt if not debit else None
    b = debit_amt * (1 if debit else -1) if debit_amt != None else None
    return d, c, b

def api_gledger_profit_and_loss_prompts():
    today = datetime.date.today()
    prior_month_end = today-datetime.timedelta(days=today.day)
    year_begin = datetime.date(prior_month_end.year, 1, 1)
    return api.PromptList(\
            date1=api.cgen.date(label='Beginning Date', default=year_begin),
            date2=api.cgen.date(label='Ending Date', default=prior_month_end),
            __order__=['date1', 'date2'])

@app.get('/api/gledger/profit-and-loss', name='api_gledger_profit_and_loss', \
        report_title='Profit & Loss', report_prompts=api_gledger_profit_and_loss_prompts)
def api_gledger_profit_and_loss():
    date1 = api.parse_date(request.query.get('date1'))
    date2 = api.parse_date(request.query.get('date2'))

    select = """
with deltas as (
    select accounts.id as account_id, sum(splits.sum) as debit
    from hacc.transactions
    join hacc.splits on transactions.tid=splits.stid
    join hacc.accounts on splits.account_id=accounts.id
    join hacc.accounttypes on accounttypes.id=accounts.type_id
    where transactions.trandate between %(d1)s and %(d2)s 
        and not accounttypes.balance_sheet
    group by accounts.id, accounts.type_id
    having sum(splits.sum)<>0
)
select accounts.id, accounts.acc_name, 
    accounttypes.sort as atype_sort,
    accounttypes.debit as debit_account, 
    accounttypes.atype_name, 
    journals.jrn_name, 
    deltas.debit
from deltas
join hacc.accounts on accounts.id=deltas.account_id
join hacc.accounttypes on accounttypes.id=accounts.type_id
join hacc.journals on journals.id=accounts.journal_id
"""

    params = {\
            'd1': date1,
            'd2': date2}
    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.pyhacc_account.surrogate(),
                acc_name=api.cgen.pyhacc_account.name(label='Account', url_key='id', represents=True),
                atype_name=api.cgen.pyhacc_accounttype.name(label='Type'),
                atype_sort=api.cgen.auto(hidden=True),
                debit_account=api.cgen.auto(hidden=True),
                jrn_name=api.cgen.pyhacc_journal.name(label='Journal'),
                debit=api.cgen.currency_usd(hidden=True),
                credit=api.cgen.currency_usd(hidden=True),
                balance=api.cgen.currency_usd())
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(data[0], insert=[('debit', 'credit', 'balance')], column_map=cm)
        def transform_dc(oldrow, row):
            d, c, b = dcb_values(row.debit, row.debit_account)
            row.debit = d
            row.credit = c
            row.balance = b
        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables['deltas', True] = columns, rows

    results.keys['report-formats'] = ['gl_summarize_by_type']
    return results.json_out()

def get_api_gledger_interval_p_and_l_prompts():
    d = datetime.date.today()
    d1 = boa.the_first(d)
    return api.PromptList(\
            ending_date=api.cgen.date(label='Ending Date', default=d1-datetime.timedelta(1)),
            intervals=api.cgen.integer(label='Intervals', default=3),
            length=api.cgen.integer(label='Months', default=6),
            __order__=['ending_date', 'intervals', 'length'])

@app.get('/api/gledger/interval-p-and-l', name='api_gledger_interval_p_and_l', \
        report_title='Interval Profit & Loss', report_prompts=get_api_gledger_interval_p_and_l_prompts)
def get_api_gledger_interval_p_and_l():
    edate = api.parse_date(request.query.get('ending_date'))
    intervals = api.parse_int(request.query.get('intervals'))
    length = api.parse_int(request.query.get('length'))

    select = """
with deltas as (
    select accounts.id as account_id, sum(splits.sum) as debit
    from hacc.transactions
    join hacc.splits on transactions.tid=splits.stid
    join hacc.accounts on splits.account_id=accounts.id
    join hacc.accounttypes on accounttypes.id=accounts.type_id
    where transactions.trandate between %(d1)s and %(d2)s 
        and not accounttypes.balance_sheet
    group by accounts.id, accounts.type_id
    having sum(splits.sum)<>0
)
select accounts.id, accounts.acc_name, 
    accounttypes.sort as atype_sort,
    accounttypes.debit as debit_account, 
    accounttypes.atype_name, 
    journals.id as jrn_id,
    journals.jrn_name, 
    deltas.debit
from deltas
join hacc.accounts on accounts.id=deltas.account_id
join hacc.accounttypes on accounttypes.id=accounts.type_id
join hacc.journals on journals.id=accounts.journal_id
"""

    ed1 = datetime.date(edate.year, edate.month, 1)
    date_ranges = []
    for index in range(intervals):
        dprior = boa.n_months_earlier(ed1, (index+1)*length)
        dcurr = boa.n_months_earlier(ed1, index*length)
        date_ranges.append((dprior, boa.month_end(dcurr)))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        intervals = [api.sql_rows(conn, select, {'d1': d1, 'd2': d2}) for d1, d2 in date_ranges]

        accounts = {}
        intsets = []
        for rowset in intervals:
            thing = {row.id: row for row in rowset}
            intsets.append(thing)
            accounts.update(thing)

        columns = [
                ('id', api.cgen.pyhacc_account.surrogate()),
                ('acc_name', api.cgen.pyhacc_account.name(url_key='id')),
                ('atype_sort', api.cgen.auto(hidden=True)),
                ('debit_account', api.cgen.boolean(hidden=True)),
                ('atype_name', api.cgen.pyhacc_accounttype.name()),
                ('jrn_id', api.cgen.pyhacc_journal.surrogate()),
                ('jrn_name', api.cgen.pyhacc_journal.name(url_key='jrn_id'))]
        for index, dates in enumerate(date_ranges):
            d1, d2 = dates
            columns += [
                    ('debit_{}'.format(index+1), api.cgen.currency_usd(label='{}\nDebit'.format(d2))),
                    ('credit_{}'.format(index+1), api.cgen.currency_usd(label='{}\nCredit'.format(d2)))]

        accrefs = list(accounts.values())
        accrefs.sort(key=lambda x: (x.atype_sort, x.acc_name))

        rtable = rtlib.ClientTable(columns, [])

        for acc in accrefs:
            with rtable.adding_row() as row:
                row.id = acc.id
                row.acc_name = acc.acc_name
                row.atype_sort = acc.atype_sort
                row.debit_account = acc.debit_account
                row.atype_name = acc.atype_name
                row.jrn_id = acc.jrn_id
                row.jrn_name = acc.jrn_name
                for index, iset in enumerate(intsets):
                    arow = iset.get(acc.id, None)
                    if arow != None:
                        d, c = dc_values(arow.debit_account, arow.debit)
                        setattr(row, 'debit_{}'.format(index+1), d)
                        setattr(row, 'credit_{}'.format(index+1), c)

        cm = {attr: values for attr, values in columns}
        results.tables['balances', True] = rtable.as_tab2(column_map=cm)

    results.keys['report-formats'] = ['gl_summarize_by_type']
    return results.json_out()

def get_api_gledger_detailed_pl_prompts():
    return api.PromptList(
            date1=api.cgen.date(label='Start Date', relevance=('date2', 'end-range', None)),
            date2=api.cgen.date(label='End Date'),
            __order__=['date1', 'date2'])

@app.get('/api/gledger/detailed-pl', name='get_api_gledger_detailed_pl', \
        report_title='Detailed Profit & Loss', report_prompts=get_api_gledger_detailed_pl_prompts)
def get_api_gledger_detailed_pl():
    date1 = api.parse_date(request.query.get('date1'))
    date2 = api.parse_date(request.query.get('date2'))

    select = """
select 
    transactions.tid, 
    transactions.trandate as date, 
    transactions.tranref as reference, 
    accounts.id, accounts.acc_name,
    transactions.payee, 
    transactions.memo, 
    case when splits.sum>=0 then splits.sum end as debit,
    case when splits.sum<0 then splits.sum end as credit
from hacc.transactions
join hacc.splits on splits.stid=transactions.tid
join hacc.accounts on splits.account_id=accounts.id
join hacc.accounttypes on accounttypes.id=accounts.type_id
where /*WHERE*/
"""

    wheres = [
            "transactions.trandate between %(d1)s and %(d2)s",
            "not accounttypes.balance_sheet"]
    params = {
            'd1': date1, 
            'd2': date2}

    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            tid=api.cgen.pyhacc_transaction.surrogate(),
            id=api.cgen.pyhacc_account.surrogate(),
            debit=api.cgen.currency_usd(),
            credit=api.cgen.currency_usd())
        results.tables['trans', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()
