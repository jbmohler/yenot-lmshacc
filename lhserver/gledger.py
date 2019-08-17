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

BALANCE_SHEET_AT_D = """
with balances as (
    select 
        accounts.id, accounts.type_id, accounts.retearn_id, 
        sum(splits.sum) as debit
    from hacc.accounts
    join hacc.splits on splits.account_id=accounts.id
    join hacc.transactions on transactions.tid=splits.stid
    where transactions.trandate<=%(d)s
    group by accounts.id, accounts.type_id, accounts.retearn_id
), balsheet as (
    select
        case when accounttypes.balance_sheet then balances.id else ret.id end as account_id, 
        balances.debit
    from balances
    join hacc.accounttypes on accounttypes.id=balances.type_id
    left outer join hacc.accounts ret on ret.id=balances.retearn_id
)
select accounts.id, accounts.acc_name, 
    split_part(accounts.description, '\n', 1) as description, 
    accounttypes.atype_name, 
    accounttypes.sort as atype_sort,
    accounttypes.debit as debit_account, 
    journals.jrn_name, 
    balsheet.debit
from (
    select balsheet.account_id, sum(balsheet.debit) as debit
    from balsheet
    group by balsheet.account_id
    having sum(balsheet.debit) <> 0.
    ) balsheet
join hacc.accounts on accounts.id=balsheet.account_id
join hacc.accounttypes on accounttypes.id=accounts.type_id
join hacc.journals on journals.id=accounts.journal_id
order by accounttypes.sort, accounts.acc_name
"""

def get_api_gledger_balance_sheet_prompts():
    return api.PromptList(\
            date=api.cgen.date(default=datetime.date.today()),
            __order__=['date'])

@app.get('/api/gledger/balance-sheet', name='api_gledger_balance_sheet', \
        report_title='Balance Sheet', report_prompts=get_api_gledger_balance_sheet_prompts)
def get_api_gledger_balance_sheet():
    date = api.parse_date(request.query.get('date'))

    select = BALANCE_SHEET_AT_D

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
        params = {\
                'd': date}
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(data[0], insert=[('debit', 'credit', 'balance')], column_map=cm)
        def transform_dc(oldrow, row):
            row.balance = row.debit * (1 if row.debit_account else -1)
            if row.debit < 0:
                row.credit = -row.debit
                row.debit = None
            else:
                row.credit = None
        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables['balances', True] = columns, rows

    results.keys['report-formats'] = ['gl_summarize_by_type']
    return results.json_out()

def get_api_gledger_triple_balance_sheet_prompts():
    d = datetime.date.today()
    d1 = boa.the_first(d)
    d2 = boa.n_months_earlier(d1, 12)
    d3 = boa.n_months_earlier(d1, 24)
    return api.PromptList(\
            d1=api.cgen.date(label='Date 1', default=d1-datetime.timedelta(1)),
            d2=api.cgen.date(label='Date 2', default=d2-datetime.timedelta(1)),
            d3=api.cgen.date(label='Date 3', default=d3-datetime.timedelta(1)),
            __order__=['d1', 'd2', 'd3'])

@app.get('/api/gledger/triple-balance-sheet', name='api_gledger_triple_balance_sheet', \
        report_title='Triple Balance Sheet', report_prompts=get_api_gledger_triple_balance_sheet_prompts)
def get_api_gledger_triple_balance_sheet():
    d1 = api.parse_date(request.query.get('d1'))
    d2 = api.parse_date(request.query.get('d2'))
    d3 = api.parse_date(request.query.get('d3'))

    select = """
with bal1 as (
    /*BAL1*/
), bal2 as (
    /*BAL2*/
), bal3 as (
    /*BAL3*/
)
select 
    coalesce(bal1.id, bal2.id, bal3.id) as id,
    coalesce(bal1.acc_name, bal2.acc_name, bal3.acc_name) as acc_name,
    coalesce(bal1.atype_name, bal2.atype_name, bal3.atype_name) as atype_name,
    coalesce(bal1.atype_sort, bal2.atype_sort, bal3.atype_sort) as atype_sort,
    coalesce(bal1.debit_account, bal2.debit_account, bal3.debit_account) as debit_account,
    coalesce(bal1.jrn_name, bal2.jrn_name, bal3.jrn_name) as jrn_name,
    bal1.debit as debit1, bal2.debit as debit2, bal3.debit as debit3
from bal1
full outer join bal2 on bal2.id=bal1.id
full outer join bal3 on bal3.id=coalesce(bal1.id, bal2.id)
order by 
    coalesce(bal1.jrn_name, bal2.jrn_name, bal3.jrn_name),
    coalesce(bal1.acc_name, bal2.acc_name, bal3.acc_name)
"""

    select = select.replace("/*BAL1*/", BALANCE_SHEET_AT_D.replace('%(d)s', '%(d1)s'))
    select = select.replace("/*BAL2*/", BALANCE_SHEET_AT_D.replace('%(d)s', '%(d2)s'))
    select = select.replace("/*BAL3*/", BALANCE_SHEET_AT_D.replace('%(d)s', '%(d3)s'))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.pyhacc_account.surrogate(),
                acc_name=api.cgen.pyhacc_account.name(label='Account', url_key='id', represents=True),
                atype_name=api.cgen.pyhacc_accounttype.name(label='Type'),
                atype_sort=api.cgen.auto(hidden=True),
                debit_account=api.cgen.auto(hidden=True),
                jrn_name=api.cgen.pyhacc_journal.name(label='Journal'),
                debit1=api.cgen.currency_usd(hidden=True),
                credit1=api.cgen.currency_usd(hidden=True),
                balance1=api.cgen.currency_usd(label='Balance\n{}'.format(d1)),
                debit2=api.cgen.currency_usd(hidden=True),
                credit2=api.cgen.currency_usd(hidden=True),
                balance2=api.cgen.currency_usd(label='Balance\n{}'.format(d2)),
                debit3=api.cgen.currency_usd(hidden=True),
                credit3=api.cgen.currency_usd(hidden=True),
                balance3=api.cgen.currency_usd(label='Balance\n{}'.format(d3)))
        params = {\
                'd1': d1,
                'd2': d2,
                'd3': d3}
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(data[0], insert=[('debit1', 'credit1', 'balance1'), ('debit2', 'credit2', 'balance2'), ('debit3', 'credit3', 'balance3')], column_map=cm)
        def transform_dc(oldrow, row):
            row.balance1 = row.debit1 * (1 if row.debit_account else -1) if row.debit1 != None else None
            if row.debit1 == None:
                row.credit1 = None
            elif row.debit1 < 0:
                row.credit1 = -row.debit1
                row.debit1 = None
            else:
                row.credit1 = None
            row.balance2 = row.debit2 * (1 if row.debit_account else -1) if row.debit2 != None else None
            if row.debit2 == None:
                row.credit2 = None
            elif row.debit2 < 0:
                row.credit2 = -row.debit2
                row.debit2 = None
            else:
                row.credit2 = None
            row.balance3 = row.debit3 * (1 if row.debit_account else -1) if row.debit3 != None else None
            if row.debit3 == None:
                row.credit3 = None
            elif row.debit3 < 0:
                row.credit3 = -row.debit3
                row.debit3 = None
            else:
                row.credit3 = None
        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables['balances', True] = columns, rows

    results.keys['report-formats'] = ['gl_summarize_by_type']
    return results.json_out()

def get_api_gledger_multi_balance_sheet_prompts():
    d = datetime.date.today()
    d1 = boa.the_first(d) - datetime.timedelta(days=1)
    months = [(datetime.date(2020, i, 1).strftime('%B'), i) for i in range(1, 13)]
    return api.PromptList(\
            year=api.cgen.basic(default=str(d1.year)),
            month_end=api.cgen.options(default=d1.month, widget_kwargs={'options': months}),
            count=api.cgen.integer(label='Periods Back', default=3),
            __order__=['year', 'month_end', 'count'])

@app.get('/api/gledger/multi-balance-sheet', name='api_gledger_multi_balance_sheet', \
        report_title='Multi Balance Sheet', report_prompts=get_api_gledger_multi_balance_sheet_prompts)
def get_api_gledger_multi_balance_sheet():
    year = api.parse_int(request.query.get('year'))
    month = api.parse_int(request.query.get('month_end'))
    count = api.parse_int(request.query.get('count', 3))

    if count <= 0:
        raise api.UserError('invalid-param', 'This report requires at least 1 interval.')

    select = """
with /*BAL_N_CTE*/
select 
    /*COALESCE_COL(id)*/ as id,
    /*COALESCE_COL(acc_name)*/ as acc_name,
    /*COALESCE_COL(atype_name)*/ as atype_name,
    /*COALESCE_COL(atype_sort)*/ as atype_sort,
    /*COALESCE_COL(debit_account)*/ as debit_account,
    /*COALESCE_COL(jrn_name)*/ as jrn_name,
    /*BAL_N_DEBIT*/
from /*BAL_N_JOINS*/
order by 
    /*COALESCE_COL(jrn_name)*/,
    /*COALESCE_COL(acc_name)*/
"""

    params = {}
    cte_list = []
    debit_list = []
    joins_list = []
    for index in range(count):
        params['d{}'.format(index)] = boa.month_end(datetime.date(year-index, month, 1))
        s = 'bal{0} as (\n\t'+BALANCE_SHEET_AT_D.replace('%(d)s', '%(d{0})s')+'\n)'
        cte_list.append(s.format(index))
        debit_list.append('bal{0}.debit as debit{0}'.format(index))
        if index == 0:
            joins_list.append('bal{0}'.format(index))
        elif index == 1:
            joins_list.append('bal{0} on bal{0}.id=bal0.id'.format(index))
        elif index >= 2:
            prior = ['bal{}.id'.format(i2) for i2 in range(index)]
            joins_list.append('bal{0} on bal{0}.id=coalesce({1})'.format(index, ', '.join(prior)))

    select = select \
            .replace("/*BAL_N_CTE*/", ", ".join(cte_list)) \
            .replace("/*BAL_N_DEBIT*/", ",\n\t".join(debit_list)) \
            .replace("/*BAL_N_JOINS*/", "\nfull outer join ".join(joins_list))

    def coalfunc(m):
        cols = ['bal{}.{}'.format(index, m.group(1)) for index in range(count)]
        return "coalesce({})".format(", ".join(cols))

    select = re.sub(r"/\*COALESCE_COL\(([a-z0-9_]+)\)\*/", coalfunc, select)

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        inserts = []
        colkwargs = {}
        for index in range(count):
            d, c, b = 'debit{}'.format(index), 'credit{}'.format(index), 'balance{}'.format(index)
            colkwargs.update({
                d: api.cgen.currency_usd(hidden=True),
                c: api.cgen.currency_usd(hidden=True),
                b: api.cgen.currency_usd(label='Balance\n{}'.format(params['d{}'.format(index)]))})
            inserts.append((d, c, b))

        cm = api.ColumnMap(\
                id=api.cgen.pyhacc_account.surrogate(),
                acc_name=api.cgen.pyhacc_account.name(label='Account', url_key='id', represents=True),
                atype_name=api.cgen.pyhacc_accounttype.name(label='Type'),
                atype_sort=api.cgen.auto(hidden=True),
                debit_account=api.cgen.auto(hidden=True),
                jrn_name=api.cgen.pyhacc_journal.name(label='Journal'),
                **colkwargs)
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(data[0], insert=inserts, column_map=cm)
        def transform_dc(oldrow, row):
            for index in range(count):
                d, c, b = 'debit{}'.format(index), 'credit{}'.format(index), 'balance{}'.format(index)

                balance = getattr(row, d) * (1 if row.debit_account else -1) if getattr(row, d) != None else None
                setattr(row, b, balance)
                if getattr(row, d) == None:
                    setattr(row, c, None)
                elif getattr(row, d) < 0:
                    setattr(row, c, -getattr(row, d))
                    setattr(row, d, None)
                else:
                    setattr(row, c, None)
        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables['balances', True] = columns, rows

    results.keys['report-formats'] = ['gl_summarize_by_type']
    return results.json_out()

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
            row.balance = row.debit * (1 if row.debit_account else -1)
            if row.debit < 0:
                row.credit = -row.debit
                row.debit = None
            else:
                row.credit = None
        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables['deltas', True] = columns, rows

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
    splits.sum as amount
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
            id=api.cgen.pyhacc_account.surrogate())
        results.tables['trans', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

def dc_values(debit, debit_amt):
    d = debit_amt if debit else None
    c = -debit_amt if not debit else None
    return d, c

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
