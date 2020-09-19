import uuid
import yenot.backend.api as api

app = api.get_global_app()


@app.get(
    "/api/journals/list", name="get_api_journals_list", report_title="Journals List"
)
def get_api_journals_list():
    select = """
select *
from hacc.journals"""

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            id=api.cgen.pyhacc_journal.surrogate(),
            jrn_name=api.cgen.pyhacc_journal.name(label="Journal", url_key="id"),
        )
        results.tables["journals", True] = api.sql_tab2(conn, select, column_map=cm)
    return results.json_out()


def _api_journal(jrn_id):
    select = """
select *
from hacc.journals
where /*WHERE*/"""

    params = {}
    if jrn_id == "new":
        select = select.replace("/*WHERE*/", "false")
    else:
        select = select.replace("/*WHERE*/", "id=%(j)s")
        params["j"] = jrn_id

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select, params)

        def defaulter(key, row):
            row.id = uuid.uuid1().hex
            row.jrn_name = "My Journal"

        if jrn_id == "new":
            rows = api.tab2_rows_default(columns, [None], defaulter)

        results.tables["journal", True] = columns, rows
    return results


@app.get("/api/journal/new", name="get_api_journal_new")
def api_journal_new():
    results = _api_journal("new")
    return results.json_out()


@app.get("/api/journal/<jrn_id>", name="get_api_journal")
def api_journal(jrn_id):
    results = _api_journal(jrn_id)
    return results.json_out()


@app.put("/api/journal/<jrn_id>", name="put_api_journal")
def put_journal(jrn_id):
    jrn = api.table_from_tab2("journal", allow_extra=True)

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows("hacc.journals", jrn)
        conn.commit()

    return api.Results().json_out()
