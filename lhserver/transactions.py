import uuid
import datetime
import json
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()


@app.get(
    "/api/transactions/years",
    name="get_api_transactions_years",
    report_title="Transaction Years",
)
def get_api_transactions_years():
    select = """
select date_part('year', trandate)::text as year, count(*)
from hacc.transactions
group by date_part('year', trandate)::text
order by date_part('year', trandate)::text
"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        results.tables["years", True] = api.sql_tab2(conn, select, None, None)
    return results.json_out()


def get_api_transactions_tran_detail_prompts():
    return api.PromptList(
        date1=api.cgen.date(label="Start Date", relevance=("date2", "end-range", None)),
        date2=api.cgen.date(label="End Date"),
        account=api.cgen.pyhacc_account.id(optional=True),
        acctype=api.cgen.pyhacc_accounttype.id(label="Account Type", optional=True),
        fragment=api.cgen.basic(optional=True),
        __order__=["date1", "date2", "account", "acctype", "fragment"],
    )


@app.get(
    "/api/transactions/tran-detail",
    name="get_api_transactions_tran_detail",
    report_title="Transaction Detail",
    report_prompts=get_api_transactions_tran_detail_prompts,
)
def get_api_transactions_tran_detail():
    date1 = api.parse_date(request.query.get("date1"))
    date2 = api.parse_date(request.query.get("date2"))
    account = request.query.get("account")
    acctype = request.query.get("acctype")
    fragment = request.query.get("fragment", None)

    if date1 == None or date2 == None:
        raise api.UserError("parameter-validation", "Enter both begin & end dates.")
    elif date1 > date2:
        raise api.UserError(
            "parameter-validation", "Start date must be before end date."
        )

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
join hacc.accounttypes on accounttypes.id=accounts.type_id
where /*WHERE*/
order by transactions.trandate, transactions.tranref, 
    transactions.payee, transactions.memo, accounttypes.sort, accounts.acc_name
"""

    results = api.Results(default_title=True)
    wheres = ["transactions.trandate between %(d1)s and %(d2)s"]
    params = {"d1": date1, "d2": date2}
    results.key_labels += f"Between {date1} and {date2}"

    if account != None:
        wheres.append("accounts.id=%(account)s")
        params["account"] = account

    if acctype != None:
        wheres.append("accounts.type_id=%(acctype)s")
        params["acctype"] = acctype

    if fragment not in ["", None]:
        params["frag"] = api.sanitize_fragment(fragment)
        wheres.append(
            "(transactions.payee ilike %(frag)s or transactions.memo ilike %(frag)s)"
        )
        results.key_labels += f'Containing "{fragment}"'

    select = select.replace("/*WHERE*/", " and ".join(wheres))

    with app.dbconn() as conn:
        cm = api.ColumnMap(
            tid=api.cgen.pyhacc_transaction.surrogate(
                row_url_label="Transaction", represents=True
            ),
            id=api.cgen.pyhacc_account.surrogate(),
            acc_name=api.cgen.pyhacc_account.name(
                url_key="id", label="Account", hidden=(account != None)
            ),
            debit=api.cgen.currency_usd(widget_kwargs={"blankzero": True}),
            credit=api.cgen.currency_usd(widget_kwargs={"blankzero": True}),
        )
        results.tables["trans", True] = api.sql_tab2(conn, select, params, cm)
        if account != None:
            accname = api.sql_1row(
                conn,
                "select acc_name from hacc.accounts where id=%(s)s",
                {"s": account},
            )
            results.key_labels += f"Account:  {accname}"
    results.keys["report-formats"] = ["gl_summarize_total"]
    return results.json_out()


def _get_api_transaction(tid=None, newrow=False, copy=False):
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
        params["t"] = tid
    if newrow:
        wheres.append("False")

    select = select.replace("/*WHERE*/", wheres[0])
    selectdet = selectdet.replace("/*WHERE*/", wheres[0])

    results = api.Results()
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            tran_status=api.cgen.auto(skip_write=True),
            tran_status_color=api.cgen.auto(skip_write=True),
        )
        columns, rows = api.sql_tab2(conn, select, params, cm)

        def tran_default(index, row):
            row.tid = str(uuid.uuid1())
            row.trandate = api.get_request_today()
            tran_status(None, row)

        def tran_clear(oldrow, row):
            row.tid = str(uuid.uuid1())
            row.trandate = api.get_request_today()
            row.receipt = None
            tran_status(oldrow, row)

        def tran_status(oldrow, row):
            if newrow:
                row.tran_status_color = "#28b463"
                row.tran_status = "New Transaction"
            elif copy:
                row.tran_status_color = "#28b463"
                row.tran_status = f"Copied from {oldrow.trandate}"
            elif row.trandate > api.get_request_today() - datetime.timedelta(days=7):
                row.tran_status_color = "#e67e22"
                row.tran_status = "Edit Recent Transaction"
            else:
                row.tran_status_color = "#a93226"
                row.tran_status = "Edit Older Transaction"

        newcols = api.tab2_columns_transform(
            columns, insert=[("tid", "tran_status_color", "tran_status")], column_map=cm
        )

        if newrow:
            rows = api.tab2_rows_default(newcols, [None], tran_default)
        elif copy:
            rows = api.tab2_rows_transform((columns, rows), newcols, tran_clear)
        else:
            rows = api.tab2_rows_transform((columns, rows), newcols, tran_status)

        results.tables["trans"] = newcols, rows

        cm = api.ColumnMap(
            sid=api.cgen.pyhacc_transplit.surrogate(primary_key=True),
            stid=api.cgen.pyhacc_transaction.surrogate(
                row_url_label="Transaction", skip_write=True
            ),
            sum=api.cgen.currency_usd(),
            account_id=api.cgen.pyhacc_account.surrogate(),
            acc_name=api.cgen.pyhacc_account.name(
                label="Account", skip_write=True, url_key="account_id"
            ),
            jrn_name=api.cgen.pyhacc_journal.name(label="Journal", skip_write=True),
        )
        columns, rows = api.sql_tab2(conn, selectdet, params, cm)
        if copy:
            parent = results.tables["trans"][1][0]

            def split_reconnect(oldrow, row):
                row.sid = str(uuid.uuid1())
                row.stid = parent.tid

            rows = api.tab2_rows_transform((columns, rows), columns, split_reconnect)
        results.tables["splits"] = columns, rows
    return results


@app.get("/api/transaction/new", name="get_api_transaction_new")
def get_api_transaction_new():
    results = _get_api_transaction(newrow=True)
    results.keys["new_row"] = True
    return results.json_out()


@app.get("/api/transaction/<t_id>", name="get_api_transaction")
def get_api_transaction(t_id):
    results = _get_api_transaction(tid=t_id)
    return results.json_out()


@app.get("/api/transaction/<t_id>/copy", name="get_api_transaction_copy")
def get_api_transaction_copy(t_id):
    results = _get_api_transaction(tid=t_id, copy=True)
    return results.json_out()


@app.put("/api/transaction/<t_id>", name="put_api_transaction")
def put_api_transaction(t_id):
    trans = api.table_from_tab2("trans", amendments=["tid"], allow_extra=True)
    splits = api.table_from_tab2(
        "splits", amendments=["stid", "sid"], required=["account_id", "sum"]
    )

    for row in trans.rows:
        row.tid = t_id
    for row in splits.rows:
        row.stid = t_id

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows("hacc.transactions", trans)
            w.upsert_rows("hacc.splits", splits)
        payload = json.dumps({"date": str(trans.rows[0].trandate)})
        api.sql_void(conn, "notify transactions, %(payload)s", {"payload": payload})
        conn.commit()
    return api.Results().json_out()


@app.delete("/api/transaction/<t_id>", name="delete_api_transaction")
def delete_api_transaction(t_id):
    with app.dbconn() as conn:
        trandate = api.sql_1row(
            conn,
            "select trandate from hacc.transactions where tid=%(tid)s",
            {"tid": t_id},
        )
        payload = json.dumps({"date": str(trandate)})
        api.sql_void(conn, "notify transactions, %(payload)s", {"payload": payload})

        api.sql_void(conn, "delete from hacc.splits where stid=%(tid)s", {"tid": t_id})
        api.sql_void(
            conn, "delete from hacc.transactions where tid=%(tid)s", {"tid": t_id}
        )
        conn.commit()
    return api.Results().json_out()
