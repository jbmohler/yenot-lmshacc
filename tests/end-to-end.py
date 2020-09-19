import os
import sys
import time
import concurrent.futures as futures
import rtlib
import yenot.client as yclient
import yenot.tests

TEST_DATABASE = "yenot_e2e_test"


def test_url(dbname):
    if "YENOT_DB_URL" in os.environ:
        return os.environ["YENOT_DB_URL"]
    # Fall back to local unix socket.  This is the url for unix domain socket.
    return "postgresql:///{}".format(dbname)


def init_database(dburl):
    r = os.system(
        "{} ../yenot/scripts/init-database.py {} --full-recreate \
            --ddl-script=schema/lmshacc.sql \
            --module=lhserver".format(
            sys.executable, dburl
        )
    )
    if r != 0:
        print("error exit")
        sys.exit(r)


def test_basic_lists(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        client.get("api/accounts/list")
        client.get("api/accounttypes/list")
        client.get("api/journals/list")
        client.get("api/transactions/list")

        session.close()


def test_crud_accounttypes(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        atcontent = client.get("api/accounttype/new")
        acctable = atcontent.named_table("accounttype")
        accrow = acctable.rows[0]
        accrow.balance_sheet = True
        accrow.atype_name = "Test Asset"

        client.put(
            "api/accounttype/{}",
            accrow.id,
            files={"accounttype": acctable.as_http_post_file()},
        )

        atcontent = client.get("api/accounttypes/list")
        found = [
            row for row in atcontent.main_table().rows if row.atype_name == "Test Asset"
        ]
        assert len(found) == 1

        session.close()


def test_crud_journals(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        atcontent = client.get("api/journal/new")
        acctable = atcontent.named_table("journal")
        accrow = acctable.rows[0]
        accrow.jrn_name = "Test Journal1"

        client.put(
            "api/journal/{}", accrow.id, files={"journal": acctable.as_http_post_file()}
        )

        atcontent = client.get("api/journals/list")
        found = [
            row
            for row in atcontent.main_table().rows
            if row.jrn_name == "Test Journal1"
        ]
        assert len(found) == 1

        session.close()


def test_crud_accounts(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        atcontent = client.get("api/accounttypes/list")
        types = atcontent.main_table()

        jrncontent = client.get("api/journals/list")
        jrns = jrncontent.main_table()

        content = client.get("api/account/new")
        acctable = content.named_table("account")
        accrow = acctable.rows[0]
        accrow.type_id = types.rows[0].id
        accrow.journal_id = jrns.rows[0].id
        accrow.description = "Test Account"

        client.put(
            "api/account/{}", accrow.id, files={"account": acctable.as_http_post_file()}
        )

        session.close()


def test_prep_data(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        jrncontent = client.get("api/journals/list")
        jrn = next(iter(jrncontent.main_table().rows))

        types = [
            dict(name="Asset", balance_sheet=True, debit=True),
            dict(name="Liability", balance_sheet=True, debit=False),
            dict(name="Capital", balance_sheet=True, debit=False),
            dict(name="Revenue", balance_sheet=False, debit=False),
            dict(name="Expense", balance_sheet=False, debit=True),
        ]

        for tt in types:
            atcontent = client.get("api/accounttype/new")
            acctable = atcontent.named_table("accounttype")
            atrow = acctable.rows[0]
            atrow.atype_name = tt["name"]
            atrow.balance_sheet = tt["balance_sheet"]
            atrow.debit = tt["debit"]

            client.put(
                "api/accounttype/{}",
                atrow.id,
                files={"accounttype": acctable.as_http_post_file()},
            )

            if tt["name"] == "Asset":
                content = client.get("api/account/new")
                acctable = content.named_table("account")
                accrow = acctable.rows[0]
                accrow.type_id = atrow.id
                accrow.journal_id = jrn.id
                accrow.acc_name = "Cash"

                client.put(
                    "api/account/{}",
                    accrow.id,
                    files={"account": acctable.as_http_post_file()},
                )
            if tt["name"] == "Expense":
                content = client.get("api/account/new")
                acctable = content.named_table("account")
                accrow = acctable.rows[0]
                accrow.type_id = atrow.id
                accrow.journal_id = jrn.id
                accrow.acc_name = "Food"

                client.put(
                    "api/account/{}",
                    accrow.id,
                    files={"account": acctable.as_http_post_file()},
                )

        content = client.get("api/accounts/by-reference", reference="Food")
        account = content.main_table().rows[0]
        assert account.account == "Food"

        content = client.get("api/accounts/completions", prefix="Fo")
        account = content.main_table().rows[0]
        assert account.acc_name == "Food"

        session.close()


def test_crud_transactions(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        acccontent = client.get("api/accounts/list")
        accs = acccontent.main_table()

        cash = next(row for row in accs.rows if row.account == "Cash")
        food = next(row for row in accs.rows if row.account == "Food")

        content = client.get("api/transaction/new")
        acctable = content.named_table("trans")
        sptable = content.named_table("splits")
        r2 = acctable.rows[0]
        r2.trandate = "2018-12-01"
        r2.payee = "Dairy Queen"
        r2.memo = "Milk Shake"
        with sptable.adding_row() as r2:
            r2.account_id = cash.id
            r2.sum = -5.25
        with sptable.adding_row() as r2:
            r2.account_id = food.id
            r2.sum = 5.25
        client.put(
            "api/transaction/{}",
            acctable.rows[0].tid,
            files={
                "trans": acctable.as_http_post_file(),
                "splits": sptable.as_http_post_file(inclusions=["account_id", "sum"]),
            },
        )

        session.close()


def test_financial_reports(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        client.get("api/gledger/unbalanced-trans")
        client.get("api/gledger/balance-sheet", date1="2019-12-31")
        client.get("api/gledger/multi-balance-sheet", year=2019, month_end=6, count=4)
        client.get(
            "api/gledger/profit-and-loss", date1="2018-01-01", date2="2018-12-31"
        )
        client.get("api/gledger/detailed-pl", date1="2018-01-01", date2="2018-12-31")
        client.get(
            "api/gledger/interval-p-and-l",
            ending_date="2018-12-31",
            intervals=3,
            length=4,
        )


if __name__ == "__main__":
    srvparams = {"dburl": test_url(TEST_DATABASE), "modules": ["lhserver"]}

    init_database(test_url(TEST_DATABASE))
    test_crud_accounttypes(srvparams)
    test_crud_journals(srvparams)
    test_crud_accounts(srvparams)
    test_prep_data(srvparams)
    test_crud_transactions(srvparams)
    test_basic_lists(srvparams)
    test_financial_reports(srvparams)
