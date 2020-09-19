import uuid
import datetime
from bottle import request
import yenot.backend.api as api

app = api.get_global_app()


def dcb_values(debit, debit_amt):
    if debit_amt == None:
        return None, None, None
    if debit_amt > 0.0:
        d = debit_amt
        c = None
    elif debit_amt < 0.0:
        d = None
        c = -debit_amt
    else:
        d = 0.0 if debit else None
        c = 0.0 if not debit else None
    b = debit_amt * (1 if debit else -1) if debit_amt != None else None
    return d, c, b


TAG_BANK_PENDING = "Bank Pending"
TAG_BANK_RECONCILED = "Bank Reconciled"


@app.get("/api/transactions/reconcile", name="get_api_transactions_reconcile")
def get_api_transactions_reconcile():
    account = request.query.get("account")

    select = """
select
    splits.sid,
    tspend.split_id is not null as pending,
    tsrec.split_id is not null as reconciled,
    splits.sum as debit,
    transactions.trandate as date,
    transactions.tranref as reference,
    transactions.payee,
    transactions.memo
from hacc.splits
left outer join hacc.tagsplits tspend on tspend.split_id=splits.sid and 
                    tspend.tag_id=(select id from hacc.tags where tag_name=%(bpend)s)
left outer join hacc.tagsplits tsrec on tsrec.split_id=splits.sid and 
                    tsrec.tag_id=(select id from hacc.tags where tag_name=%(brec)s)
join hacc.transactions on splits.stid=transactions.tid
where splits.account_id=%(account)s and tsrec.split_id is null
"""

    select_acc = """
select
    accounts.id, accounts.acc_name,
    accounts.rec_note,
    atype.debit as debit_account,
    coalesce(reconciled.summary, 0.0) * (case when atype.debit then 1 else -1 end) as prior_reconciled_balance
from hacc.accounts
join hacc.accounttypes atype on atype.id=accounts.type_id
left outer join lateral (
    select sum(splits.sum) as summary
    from hacc.splits
    join hacc.tagsplits tsrec on tsrec.split_id=splits.sid and 
                        tsrec.tag_id=(select id from hacc.tags where tag_name=%(brec)s)
    where splits.account_id=accounts.id
    ) reconciled on true
where accounts.id=%(account)s"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        params = {
            "account": account,
            "bpend": TAG_BANK_PENDING,
            "brec": TAG_BANK_RECONCILED,
        }

        cm = api.ColumnMap(summary=api.cgen.currency_usd())
        results.tables["account"] = api.sql_tab2(conn, select_acc, params, cm)
        acnt = results.tables["account"][1][0]

        cm = api.ColumnMap(
            sid=api.cgen.__meta__(),
            debit=api.cgen.currency_usd(),
            credit=api.cgen.currency_usd(),
            balance=api.cgen.currency_usd(hidden=True),
        )
        data = api.sql_tab2(conn, select, params, cm)

        columns = api.tab2_columns_transform(
            data[0], insert=[("debit", "credit", "balance")], column_map=cm
        )

        def transform_dc(oldrow, row):
            nonlocal acnt
            d, c, b = dcb_values(acnt.debit_account, row.debit)
            row.balance = b
            row.debit = d
            row.credit = c

        rows = api.tab2_rows_transform(data, columns, transform_dc)
        results.tables["trans"] = columns, rows
    return results.json_out()


@app.put("/api/transactions/reconcile", name="put_api_transactions_reconcile")
def put_api_transactions_reconcile():
    trans = api.table_from_tab2("trans", required=["sid", "pending", "reconciled"])
    account = api.table_from_tab2("account", required=["id"], options=["rec_note"])

    delete_pending = """
with SPLIT_KEYS
delete from hacc.tagsplits where (tag_id, split_id) in 
(select tags.id, sid::uuid
from splitkeys
join hacc.tags on tags.tag_name=%(tagspec)s
where not splitkeys./*COLUMN*/)
"""
    insert_pending = """
with SPLIT_KEYS
insert into hacc.tagsplits (tag_id, split_id)
select tags.id, sid::uuid
from splitkeys
join hacc.tags on tags.tag_name=%(tagspec)s
where splitkeys./*COLUMN*/
on conflict (tag_id, split_id) do nothing
"""

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            # w.upsert_rows('hacc.transactions', trans)
            w.upsert_rows("hacc.accounts", account)

        x = trans.as_cte(conn, "splitkeys")

        api.sql_void(
            conn,
            delete_pending.replace("SPLIT_KEYS", x).replace("/*COLUMN*/", "pending"),
            {"tagspec": TAG_BANK_PENDING},
        )
        api.sql_void(
            conn,
            insert_pending.replace("SPLIT_KEYS", x).replace("/*COLUMN*/", "pending"),
            {"tagspec": TAG_BANK_PENDING},
        )
        api.sql_void(
            conn,
            delete_pending.replace("SPLIT_KEYS", x).replace("/*COLUMN*/", "reconciled"),
            {"tagspec": TAG_BANK_RECONCILED},
        )
        api.sql_void(
            conn,
            insert_pending.replace("SPLIT_KEYS", x).replace("/*COLUMN*/", "reconciled"),
            {"tagspec": TAG_BANK_RECONCILED},
        )

        conn.commit()

    return api.Results().json_out()
