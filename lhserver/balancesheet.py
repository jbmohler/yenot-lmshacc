import datetime
import re
import yenot.backend.api as api
from . import shared
from . import bankday

app = api.get_global_app()


def dcb_values(debit, debit_amt):
    if debit_amt == None:
        return None, None, None
    d = debit_amt if debit else None
    c = -debit_amt if not debit else None
    b = debit_amt * (1 if debit else -1) if debit_amt != None else None
    return d, c, b


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
select
    accounttypes.id as atype_id, 
    accounttypes.atype_name, 
    accounttypes.sort as atype_sort,
    accounttypes.debit as debit_account, 
    journals.id as jrn_id,
    journals.jrn_name, 
    accounts.id, accounts.acc_name, 
    accounts.description,
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
"""


def get_api_gledger_balance_sheet_prompts():
    return api.PromptList(
        date=api.cgen.date(default=api.get_request_today()), __order__=["date"]
    )


@app.get(
    "/api/gledger/balance-sheet",
    name="api_gledger_balance_sheet",
    report_title="Balance Sheet",
    report_prompts=get_api_gledger_balance_sheet_prompts,
    report_sidebars=shared.account_sidebar("id"),
)
def get_api_gledger_balance_sheet(request):
    date = api.parse_date(request.query.get("date"))

    select = BALANCE_SHEET_AT_D

    results = api.Results(default_title=True)
    results.key_labels += f"Date:  {date}"
    with app.dbconn() as conn:
        cm = shared.HaccColumnMap(
            id=api.cgen.pyhacc_account.surrogate(),
            acc_name=api.cgen.pyhacc_account.name(
                label="Account", url_key="id", represents=True
            ),
            atype_id=api.cgen.pyhacc_accounttype.surrogate(),
            atype_name=api.cgen.pyhacc_accounttype.name(
                label="Account Type", url_key="atype_id", sort_proxy="atype_sort"
            ),
            atype_sort=api.cgen.auto(hidden=True),
            debit_account=api.cgen.auto(hidden=True),
            jrn_id=api.cgen.pyhacc_journal.surrogate(),
            jrn_name=api.cgen.pyhacc_journal.name(label="Journal", url_key="jrn_id"),
            debit=api.cgen.currency_usd(hidden=True),
            credit=api.cgen.currency_usd(hidden=True),
            balance=api.cgen.currency_usd(),
        )
        params = {"d": date}
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(
            data[0], insert=[("debit", "credit", "balance")], column_map=cm
        )

        def transform_dc(oldrow, row):
            d, c, b = dcb_values(row.debit_account, row.debit)
            row.balance = b
            row.debit = d
            row.credit = c

        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables["balances", True] = columns, rows

    results.keys["report-formats"] = ["gl_summarize_by_type"]
    results.keys["report-refresh"] = [{"channel": "transactions"}]
    return results.json_out()


def get_api_gledger_balance_sheet_summary_prompts():
    return api.PromptList(
        date=api.cgen.date(default=api.get_request_today()), __order__=["date"]
    )


@app.get(
    "/api/gledger/balance-sheet-summary",
    name="api_gledger_balance_sheet_summary",
    report_title="Balance Sheet Summary",
    report_prompts=get_api_gledger_balance_sheet_summary_prompts,
)
def get_api_gledger_balance_sheet_summary(request):
    date = api.parse_date(request.query.get("date"))

    select = f"""
with bsacc as (
    {BALANCE_SHEET_AT_D}
)
select 
    bsacc.jrn_id,
    bsacc.jrn_name,
    bsacc.atype_id,
    bsacc.atype_name,
    bsacc.atype_sort,
    bsacc.debit_account,
    sum(bsacc.debit) as debit
from bsacc
group by 
    bsacc.jrn_id,
    bsacc.jrn_name,
    bsacc.atype_id,
    bsacc.atype_name,
    bsacc.atype_sort,
    bsacc.debit_account
order by bsacc.jrn_name, bsacc.atype_sort
"""

    results = api.Results(default_title=True)
    results.key_labels += f"Date:  {date}"
    with app.dbconn() as conn:
        cm = shared.HaccColumnMap(
            atype_id=api.cgen.pyhacc_accounttype.surrogate(),
            atype_name=api.cgen.pyhacc_accounttype.name(
                label="Account Type", url_key="atype_id", sort_proxy="atype_sort"
            ),
            atype_sort=api.cgen.auto(hidden=True),
            debit_account=api.cgen.auto(hidden=True),
            jrn_id=api.cgen.pyhacc_journal.surrogate(),
            jrn_name=api.cgen.pyhacc_journal.name(label="Journal", url_key="jrn_id"),
            debit=api.cgen.currency_usd(),
            credit=api.cgen.currency_usd(),
            balance=api.cgen.currency_usd(hidden=True),
        )
        params = {"d": date}
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(
            data[0], insert=[("debit", "credit", "balance")], column_map=cm
        )

        def transform_dc(oldrow, row):
            d, c, b = dcb_values(row.debit_account, row.debit)
            row.balance = b
            row.debit = d
            row.credit = c

        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables["balances", True] = columns, rows

    results.keys["report-formats"] = ["gl_summarize_by_type"]
    results.keys["report-refresh"] = [{"channel": "transactions"}]
    return results.json_out()


def get_api_gledger_current_balance_accounts_prompts():
    return api.PromptList(
        date=api.cgen.date(default=api.get_request_today()), __order__=["date"]
    )


@app.get(
    "/api/gledger/current-balance-accounts",
    name="api_gledger_current_balance_accounts",
    report_title="Current Balance Accounts",
    report_prompts=get_api_gledger_current_balance_accounts_prompts,
    report_sidebars=shared.account_sidebar("id"),
)
def get_api_gledger_current_balance_accounts(request):
    date = api.parse_date(request.query.get("date"))

    select = """
with balance as (
    /*BALANCE_SHEET_AT_D*/
), recent as (
    select distinct accounts.id
    from hacc.accounts
    join hacc.splits on splits.account_id=accounts.id
    join hacc.transactions on transactions.tid=splits.stid
    where transactions.trandate between %(d)s-interval '30 days' and %(d)s+interval '30 days'
)
select
    accounttypes.id as atype_id, 
    accounttypes.atype_name, 
    accounttypes.sort as atype_sort,
    accounttypes.debit as debit_account,
    journals.id as jrn_id, journals.jrn_name,
    accounts.id, accounts.acc_name,
    accounts.description,
    balance.debit
from hacc.accounts
left outer join hacc.journals on journals.id=accounts.journal_id
left outer join hacc.accounttypes on accounttypes.id=accounts.type_id
left outer join balance on balance.id=accounts.id
where accounts.id in ((select id from balance)union(select id from recent)) and 
    accounttypes.balance_sheet
order by accounttypes.sort, journals.jrn_name, accounts.acc_name
"""

    select = select.replace("/*BALANCE_SHEET_AT_D*/", BALANCE_SHEET_AT_D)

    results = api.Results(default_title=True)
    results.key_labels += f"Date:  {date}"
    with app.dbconn() as conn:
        cm = shared.HaccColumnMap(
            id=api.cgen.pyhacc_account.surrogate(),
            acc_name=api.cgen.pyhacc_account.name(
                label="Account", url_key="id", represents=True
            ),
            atype_id=api.cgen.pyhacc_accounttype.surrogate(),
            atype_name=api.cgen.pyhacc_accounttype.name(
                label="Account Type", url_key="atype_id", sort_proxy="atype_sort"
            ),
            atype_sort=api.cgen.auto(hidden=True),
            debit_account=api.cgen.auto(hidden=True),
            jrn_id=api.cgen.pyhacc_journal.surrogate(),
            jrn_name=api.cgen.pyhacc_journal.name(label="Journal", url_key="jrn_id"),
            debit=api.cgen.currency_usd(hidden=True),
            credit=api.cgen.currency_usd(hidden=True),
            balance=api.cgen.currency_usd(),
        )
        params = {"d": date}
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(
            data[0], insert=[("debit", "credit", "balance")], column_map=cm
        )

        def transform_dc(oldrow, row):
            d, c, b = dcb_values(row.debit_account, row.debit)
            row.balance = b
            row.debit = d
            row.credit = c

        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables["balances", True] = columns, rows

    results.keys["report-formats"] = ["gl_summarize_by_type"]
    # ultimately, this calls back to api/transactions/reconcile
    results.keys["client-row-relateds"] = [
        ("Reconcile", "pyhacc:reconcile", {}, {"account": "id"})
    ]
    results.keys["report-refresh"] = [{"channel": "transactions"}]
    return results.json_out()


def get_api_gledger_multi_balance_sheet_prompts():
    d = api.get_request_today()
    d1 = bankday.the_first(d) - datetime.timedelta(days=1)
    months = [(datetime.date(2020, i, 1).strftime("%B"), i) for i in range(1, 13)]
    return api.PromptList(
        year=api.cgen.basic(default=str(d1.year)),
        month_end=api.cgen.options(default=d1.month, widget_kwargs={"options": months}),
        count=api.cgen.integer(label="Periods Back", default=3),
        __order__=["year", "month_end", "count"],
    )


@app.get(
    "/api/gledger/multi-balance-sheet",
    name="api_gledger_multi_balance_sheet",
    report_title="Balance Sheet - Comparative",
    report_prompts=get_api_gledger_multi_balance_sheet_prompts,
    report_sidebars=shared.account_sidebar("id"),
)
def get_api_gledger_multi_balance_sheet(request):
    year = api.parse_int(request.query.get("year"))
    month = api.parse_int(request.query.get("month_end"))
    count = api.parse_int(request.query.get("count", 3))

    if count <= 0:
        raise api.UserError(
            "invalid-param", "This report requires at least 1 interval."
        )

    select = """
with /*BAL_N_CTE*/
select 
    /*COALESCE_COL(atype_id)*/ as atype_id,
    /*COALESCE_COL(atype_name)*/ as atype_name,
    /*COALESCE_COL(atype_sort)*/ as atype_sort,
    /*COALESCE_COL(debit_account)*/ as debit_account,
    /*COALESCE_COL(jrn_id)*/ as jrn_id,
    /*COALESCE_COL(jrn_name)*/ as jrn_name,
    /*COALESCE_COL(id)*/ as id,
    /*COALESCE_COL(acc_name)*/ as acc_name,
    /*COALESCE_COL(description)*/ as description,
    /*BAL_N_DEBIT*/
from /*BAL_N_JOINS*/
order by 
    /*COALESCE_COL(atype_sort)*/,
    /*COALESCE_COL(jrn_name)*/,
    /*COALESCE_COL(acc_name)*/
"""

    params = {}
    cte_list = []
    debit_list = []
    joins_list = []
    for index in range(count):
        params[f"d{index}"] = bankday.month_end(datetime.date(year - index, month, 1))
        s = "bal{0} as (\n\t" + BALANCE_SHEET_AT_D.replace("%(d)s", "%(d{0})s") + "\n)"
        cte_list.append(s.format(index))
        debit_list.append("bal{0}.debit as debit{0}".format(index))
        if index == 0:
            joins_list.append(f"bal{index}")
        elif index == 1:
            joins_list.append("bal{0} on bal{0}.id=bal0.id".format(index))
        elif index >= 2:
            prior = [f"bal{i2}.id" for i2 in range(index)]
            joins_list.append(
                "bal{0} on bal{0}.id=coalesce({1})".format(index, ", ".join(prior))
            )

    select = (
        select.replace("/*BAL_N_CTE*/", ", ".join(cte_list))
        .replace("/*BAL_N_DEBIT*/", ",\n\t".join(debit_list))
        .replace("/*BAL_N_JOINS*/", "\nfull outer join ".join(joins_list))
    )

    def coalfunc(m):
        cols = [f"bal{index}.{m.group(1)}" for index in range(count)]
        return f"coalesce({', '.join(cols)})"

    select = re.sub(r"/\*COALESCE_COL\(([a-z0-9_]+)\)\*/", coalfunc, select)

    results = api.Results(default_title=True)
    results.key_labels += f"Date:  {params['d0']} and {count - 1} annual comparisons"
    with app.dbconn() as conn:
        inserts = []
        colkwargs = {}
        for index in range(count):
            d, c, b = (
                f"debit{index}",
                f"credit{index}",
                f"balance{index}",
            )
            colkwargs.update(
                {
                    d: api.cgen.currency_usd(hidden=True),
                    c: api.cgen.currency_usd(hidden=True),
                    b: api.cgen.currency_usd(label=f"Balance\n{params[f'd{index}']}"),
                }
            )
            inserts.append((d, c, b))

        cm = shared.HaccColumnMap(
            id=api.cgen.pyhacc_account.surrogate(),
            acc_name=api.cgen.pyhacc_account.name(
                label="Account", url_key="id", represents=True
            ),
            description=api.cgen.auto(),
            atype_id=api.cgen.pyhacc_accounttype.surrogate(),
            atype_name=api.cgen.pyhacc_accounttype.name(
                label="Account Type", url_key="atype_id", sort_proxy="atype_sort"
            ),
            atype_sort=api.cgen.auto(hidden=True),
            debit_account=api.cgen.auto(hidden=True),
            jrn_id=api.cgen.pyhacc_journal.surrogate(),
            jrn_name=api.cgen.pyhacc_journal.name(label="Journal", url_key="jrn_id"),
            **colkwargs,
        )
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(data[0], insert=inserts, column_map=cm)

        def transform_dc(oldrow, row):
            for index in range(count):
                d_at, c_at, b_at = (
                    f"debit{index}",
                    f"credit{index}",
                    f"balance{index}",
                )

                d, c, b = dcb_values(row.debit_account, getattr(row, d_at))
                setattr(row, b_at, b)
                setattr(row, d_at, d)
                setattr(row, c_at, c)

        rows = api.tab2_rows_transform(data, columns, transform_dc)

        results.tables["balances", True] = columns, rows

    results.keys["report-formats"] = ["gl_summarize_by_type"]
    results.keys["report-refresh"] = [{"channel": "transactions"}]
    return results.json_out()
